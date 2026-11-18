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

// keyLocks serializes checkouts with the same Idempotency-Key inside this
// process. The UNIQUE constraint in Postgres is the cross-replica backstop.
type keyLocks struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

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

	// 1. Idempotency gate: a completed key always replays the stored response.
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

	// 2. Validate active reservation and read the price from Postgres.
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

	// 3. Mock payment: 500ms latency, 5% failures.
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

	// 4. Atomic stock decrement + order + idempotency record in one transaction.
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
		// Documented trade-off: no transactional outbox in this exercise.
		log.Printf("publish %s (key=%s): %v", topic, key, err)
		return
	}
	log.Printf("published %s key=%s", topic, key)
}
