package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
)

// product.go expone el catálogo y es la única ventana de la UI al stock.
// Postgres manda: Redis guarda una copia por 30 segundos para no golpear la base
// en cada refresco, pero nadie decide vender leyendo esa copia.

// Product es la proyección que consume la tienda. El stock sale de Postgres;
// Redis sólo cachea esta vista durante 30 segundos.
type Product struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	PriceCents  int    `json:"price_cents"`
	Stock       int    `json:"stock"`
	Emoji       string `json:"emoji"`
}

const productColumns = `id, name, description, price_cents, stock, emoji`

func scanProduct(row rowScanner) (Product, error) {
	var p Product
	err := row.Scan(&p.ID, &p.Name, &p.Description, &p.PriceCents, &p.Stock, &p.Emoji)
	return p, err
}

func (a *App) getProduct(ctx context.Context, id int) (Product, error) {
	return scanProduct(a.db.QueryRowContext(ctx,
		`SELECT `+productColumns+` FROM products WHERE id = $1`, id))
}

func (a *App) listProducts(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	ctx := r.Context()

	if cached, err := a.rdb.Get(ctx, productsCacheKey).Bytes(); err == nil {
		var products []Product
		if json.Unmarshal(cached, &products) == nil {
			w.Header().Set("X-Cache", "HIT")
			writeJSON(w, http.StatusOK, products)
			return
		}
	}

	rows, err := a.db.QueryContext(ctx,
		`SELECT `+productColumns+` FROM products ORDER BY id`)
	if err != nil {
		serverError(w, err)
		return
	}
	defer rows.Close()

	products := make([]Product, 0, 10)
	for rows.Next() {
		p, err := scanProduct(rows)
		if err != nil {
			serverError(w, err)
			return
		}
		products = append(products, p)
	}
	if err := rows.Err(); err != nil {
		serverError(w, err)
		return
	}

	if encoded, err := json.Marshal(products); err == nil {
		if err := a.rdb.Set(ctx, productsCacheKey, encoded, productsCacheTTL).Err(); err != nil {
			log.Printf("cache products: %v", err)
		}
	}
	w.Header().Set("X-Cache", "MISS")
	writeJSON(w, http.StatusOK, products)
}

func (a *App) invalidateProductsCache(ctx context.Context) {
	if err := a.rdb.Del(ctx, productsCacheKey).Err(); err != nil {
		log.Printf("invalidate products cache: %v", err)
	}
}

// getStock se usa como techo de la reserva. Postgres es la fuente de verdad;
// Redis nunca inventa capacidad.
func (a *App) getStock(ctx context.Context, productID int) (int, error) {
	var stock int
	err := a.db.QueryRowContext(ctx, `SELECT stock FROM products WHERE id = $1`, productID).Scan(&stock)
	if err == sql.ErrNoRows {
		return 0, sql.ErrNoRows
	}
	return stock, err
}

func (a *App) getReservation(ctx context.Context, ref cartRef) (int, error) {
	value, err := a.rdb.Get(ctx, cartKey(ref)).Int()
	if err != nil {
		return 0, err
	}
	return value, nil
}

// resetStock es un helper de desarrollo para repetir el experimento de los 100
// usuarios sin reconstruir el stack. La ruta sólo se registra cuando
// ENABLE_DEV_ENDPOINTS=true: en producción no existe, no responde 403, directamente
// no está en el mux.
func (a *App) resetStock(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	ctx := r.Context()
	if _, err := a.db.ExecContext(ctx, `UPDATE products SET stock = 10 WHERE id = 1`); err != nil {
		serverError(w, err)
		return
	}
	zkey, hkey := reservationIndexKeys(1)
	keys := []string{zkey, hkey, productsCacheKey}
	iter := a.rdb.Scan(ctx, 0, "cart:*:1", 100).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		serverError(w, err)
		return
	}
	if len(keys) > 0 {
		if err := a.rdb.Del(ctx, keys...).Err(); err != nil {
			serverError(w, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "reset", "product_id": 1, "stock": 10})
}
