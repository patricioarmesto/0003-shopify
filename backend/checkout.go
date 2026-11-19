package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// keyLocks serializa dentro de este proceso los checkouts que comparten una
// Idempotency-Key. Es lo que hace que el doble click devuelva una sola orden:
// el segundo request espera a que el primero termine y encuentra la clave ya
// guardada. No alcanza con varias réplicas: para ese caso está el UNIQUE de
// Postgres, que se comprueba más abajo como respaldo.
type keyLocks struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// get devuelve un mutex por clave, no uno global. Así los checkouts de
// usuarios distintos siguen ocurriendo en paralelo durante el pico.
func (k *keyLocks) get(key string) *sync.Mutex {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.locks == nil {
		k.locks = make(map[string]*sync.Mutex)
	}
	if k.locks[key] == nil {
		k.locks[key] = &sync.Mutex{}
	}
	return k.locks[key]
}

type Order struct {
	ID             string    `json:"id"`
	IdempotencyKey string    `json:"idempotency_key"`
	UserID         string    `json:"user_id"`
	ProductID      int       `json:"product_id"`
	ProductName    string    `json:"product_name"`
	Qty            int       `json:"qty"`
	TotalCents     int       `json:"total_cents"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
}

type CheckoutResponse struct {
	Status     string `json:"status"`
	Order      *Order `json:"order,omitempty"`
	Message    string `json:"message,omitempty"`
	Idempotent bool   `json:"idempotent,omitempty"`
}

// checkout es el corazón del ejercicio. El orden importa: primero se busca la
// clave, luego se valida la reserva, después se cobra y al final se escribe
// todo en una transacción. Mover un paso cambia las garantías: cobrar antes de
// reservar cobraría sin stock, y escribir antes de cobrar dejaría órdenes de
// pagos rechazados.
func (a *App) checkout(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}

	var req CartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if message := req.validationError(); message != "" {
		writeError(w, http.StatusBadRequest, message)
		return
	}
	ref := req.ref()

	lock := a.locks.get(key)
	lock.Lock()
	defer lock.Unlock()

	ctx := r.Context()

	// 1. Si la clave ya terminó, se devuelve la respuesta guardada palabra por
	// palabra. No se vuelve a descontar stock ni se vuelve a cobrar. La cabecera
	// X-Idempotent-Replay existe para que el estudiante vea que hubo replay.
	var stored []byte
	err := a.db.QueryRowContext(ctx,
		`SELECT response FROM idempotency_keys WHERE key = $1`, key).Scan(&stored)
	if err == nil {
		w.Header().Set("X-Idempotent-Replay", "true")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(stored)
		return
	}
	if !errors.Is(err, sql.ErrNoRows) {
		serverError(w, err)
		return
	}

	// 2. Sin reserva activa no se cobra. El precio se lee acá para calcular el
	// total del pago mock, pero se vuelve a leer dentro de la transacción:
	// entre el cobro y el COMMIT otro request pudo cambiar el catálogo.
	product, err := a.getProduct(ctx, req.ProductID)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "product not found")
		return
	}
	if err != nil {
		serverError(w, err)
		return
	}

	reservedQty, err := a.getReservation(ctx, ref)
	if err != nil || reservedQty < req.Qty {
		writeJSON(w, http.StatusConflict, CheckoutResponse{
			Status:  "no_reservation",
			Message: "La reserva no existe o expiró. Reservá antes de pagar.",
		})
		return
	}
	totalCents := product.PriceCents * req.Qty

	// 3. El pago tarda 500 ms y falla el 5 % de las veces. Si falla se libera la
	// reserva para que ese stock vuelva a estar disponible y se publica
	// payment_failed. No se guarda clave de idempotencia: no hubo cobro ni orden,
	// así que reintentar con la misma clave es legítimo tras reservar de nuevo.
	result := a.payment.Charge(key, totalCents)
	if !result.OK {
		if err := a.releaseReservation(ctx, ref); err != nil {
			log.Printf("release reservation after failure: %v", err)
		}
		a.publish(ctx, topicPaymentFailed, key, map[string]any{
			"idempotency_key": key,
			"user_id":         req.UserID,
			"product_id":      req.ProductID,
			"qty":             req.Qty,
			"total_cents":     totalCents,
			"reason":          result.FailureReason,
			"occurred_at":     time.Now().UTC(),
		})
		writeJSON(w, http.StatusPaymentRequired, CheckoutResponse{
			Status:  "payment_failed",
			Message: result.FailureReason,
		})
		return
	}

	// 4. Descuento de stock, orden y registro de idempotencia entran en la MISMA
	// transacción. Si algo falla hay rollback: no queda ni stock descontado ni
	// orden huérfana. El WHERE stock >= qty evita vender de más aunque cien
	// requests lleguen al mismo tiempo.
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		serverError(w, err)
		return
	}

	var productName string
	var priceCents int
	err = tx.QueryRowContext(ctx, `
		UPDATE products SET stock = stock - $1
		WHERE id = $2 AND stock >= $1
		RETURNING name, price_cents`, req.Qty, req.ProductID).
		Scan(&productName, &priceCents)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		_ = a.releaseReservation(ctx, ref)
		writeJSON(w, http.StatusConflict, CheckoutResponse{
			Status:  "out_of_stock",
			Message: "Sin stock",
		})
		return
	}
	if err != nil {
		_ = tx.Rollback()
		serverError(w, err)
		return
	}
	totalCents = priceCents * req.Qty

	order := Order{
		IdempotencyKey: key,
		UserID:         req.UserID,
		ProductID:      req.ProductID,
		ProductName:    productName,
		Qty:            req.Qty,
		TotalCents:     totalCents,
		Status:         "paid",
	}
	err = tx.QueryRowContext(ctx, `
		INSERT INTO orders
			(idempotency_key, user_id, product_id, product_name, qty, total_cents, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, 'paid', $7)
		RETURNING id, created_at`,
		key, req.UserID, req.ProductID, productName, req.Qty, totalCents, time.Now().UTC()).
		Scan(&order.ID, &order.CreatedAt)
	if err != nil {
		_ = tx.Rollback()
		// Respaldo entre réplicas: dos procesos pueden pasar el mutex local y el
		// SELECT previo al mismo tiempo. El UNIQUE de Postgres decide y el que
		// pierde devuelve la orden que ya existe.
		if pqUniqueViolation(err) {
			existing, findErr := a.orderByKey(ctx, key)
			if findErr != nil {
				serverError(w, findErr)
				return
			}
			writeJSON(w, http.StatusOK, CheckoutResponse{
				Status: "paid", Order: &existing, Idempotent: true,
			})
			return
		}
		serverError(w, err)
		return
	}

	response := CheckoutResponse{Status: "paid", Order: &order}
	encoded, err := json.Marshal(response)
	if err != nil {
		_ = tx.Rollback()
		serverError(w, err)
		return
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO idempotency_keys (key, order_id, status, response)
		VALUES ($1, $2, 'paid', $3::jsonb)`, key, order.ID, string(encoded))
	if err != nil {
		_ = tx.Rollback()
		serverError(w, err)
		return
	}
	if err := tx.Commit(); err != nil {
		serverError(w, err)
		return
	}

	// Confirmado el COMMIT: la reserva ya no sirve, la caché del catálogo hay
	// que invalidarla para que se vea el stock nuevo, y se publica el evento.
	// Publicar DESPUÉS del commit deja una ventana de fallo; ADR-005 explica por
	// qué se acepta en este ejercicio.
	_ = a.releaseReservation(ctx, ref)
	a.invalidateProductsCache(ctx)
	a.publish(ctx, topicOrderCreated, order.ID, SaleEvent{
		OrderID:     order.ID,
		ProductID:   order.ProductID,
		ProductName: order.ProductName,
		Qty:         order.Qty,
		TotalCents:  order.TotalCents,
		Status:      order.Status,
		CreatedAt:   order.CreatedAt,
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(encoded)
}

func (a *App) orderByKey(ctx context.Context, key string) (Order, error) {
	return scanOrder(a.db.QueryRowContext(ctx, `
		SELECT id, idempotency_key, user_id, product_id, product_name,
		       qty, total_cents, status, created_at
		FROM orders WHERE idempotency_key = $1`, key))
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanOrder(row rowScanner) (Order, error) {
	var o Order
	err := row.Scan(&o.ID, &o.IdempotencyKey, &o.UserID, &o.ProductID,
		&o.ProductName, &o.Qty, &o.TotalCents, &o.Status, &o.CreatedAt)
	return o, err
}

func (a *App) listOrders(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	rows, err := a.db.QueryContext(r.Context(), `
		SELECT id, idempotency_key, user_id, product_id, product_name,
		       qty, total_cents, status, created_at
		FROM orders ORDER BY created_at DESC LIMIT 100`)
	if err != nil {
		serverError(w, err)
		return
	}
	defer rows.Close()

	orders := make([]Order, 0)
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			serverError(w, err)
			return
		}
		orders = append(orders, o)
	}
	if err := rows.Err(); err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, orders)
}

func (a *App) publish(ctx context.Context, topic, key string, value any) {
	payload, err := json.Marshal(value)
	if err != nil {
		log.Printf("encode %s: %v", topic, err)
		return
	}
	err = a.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: payload,
	})
	if err != nil {
		// Si Kafka no responde, sólo queda el log: la orden ya está confirmada.
		// No hay transactional outbox en este ejercicio (ver ADR-005).
		log.Printf("publish %s (key=%s): %v", topic, key, err)
		return
	}
	log.Printf("published %s key=%s", topic, key)
}
