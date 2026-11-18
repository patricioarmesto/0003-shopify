package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

const (
	productsCacheKey   = "products:list"
	productsCacheTTL   = 30 * time.Second
	reservationTTL     = 10 * time.Minute
	topicOrderCreated  = "order_created"
	topicPaymentFailed = "payment_failed"
)

type App struct {
	db      *sql.DB
	rdb     *redis.Client
	writer  *kafka.Writer
	payment *PaymentService
	locks   keyLocks
	ch      clickHouseClient
}

type SaleEvent struct {
	OrderID     string    `json:"order_id"`
	ProductID   int       `json:"product_id"`
	ProductName string    `json:"product_name"`
	Qty         int       `json:"qty"`
	TotalCents  int       `json:"total_cents"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
}

type SalePoint struct {
	Minute  string  `json:"minute"`
	Orders  int64   `json:"orders"`
	Revenue float64 `json:"revenue"`
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func main() {
	worker := flagWorker()
	if worker {
		runWorker(context.Background())
		return
	}
	runAPI(context.Background())
}

// flagWorker keeps main.go dependency-light while still allowing the same
// image to run as the API or as the Kafka -> ClickHouse order worker.
func flagWorker() bool {
	for _, arg := range os.Args[1:] {
		if arg == "-worker" || arg == "--worker" {
			return true
		}
	}
	return false
}

func openDB(ctx context.Context) *sql.DB {
	dsn := env("PG_DSN", "postgres://shopify:shopify@localhost:5432/shopify?sslmode=disable")
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	db.SetMaxOpenConns(60)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(30 * time.Minute)

	for attempt := 1; attempt <= 60; attempt++ {
		if err := db.PingContext(ctx); err == nil {
			log.Printf("postgres connected")
			return db
		}
		log.Printf("waiting for postgres (attempt %d)", attempt)
		time.Sleep(time.Second)
	}
	log.Fatal("postgres did not become ready")
	return nil
}

func openRedis(ctx context.Context) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     env("REDIS_ADDR", "localhost:6379"),
		Password: env("REDIS_PASSWORD", ""),
	})
	for attempt := 1; attempt <= 60; attempt++ {
		if err := rdb.Ping(ctx).Err(); err == nil {
			log.Printf("redis connected")
			return rdb
		}
		log.Printf("waiting for redis (attempt %d)", attempt)
		time.Sleep(time.Second)
	}
	log.Fatal("redis did not become ready")
	return nil
}

func newKafkaWriter() *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")...),
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
	}
}

func newKafkaReader() *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		Topic:          topicOrderCreated,
		GroupID:        "order-analytics",
		MinBytes:       1,
		MaxBytes:       10_000_000,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: 0, // manual commit after the ClickHouse insert succeeds
		MaxWait:        500 * time.Millisecond,
	})
}

func runAPI(ctx context.Context) {
	app := &App{
		db:      openDB(ctx),
		rdb:     openRedis(ctx),
		writer:  newKafkaWriter(),
		payment: NewPaymentService(),
		ch:      newClickHouseClient(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("/api/products", app.listProducts)
	mux.HandleFunc("/api/cart/reserve", app.reserveProduct)
	if env("ENABLE_DEV_ENDPOINTS", "false") == "true" {
		mux.HandleFunc("/api/dev/reset", app.resetStock)
	}
	mux.HandleFunc("/api/checkout", app.checkout)
	mux.HandleFunc("/api/orders", app.listOrders)
	mux.HandleFunc("/api/analytics/sales", app.sales)

	port := env("PORT", "8080")
	log.Printf("API listening on :%s", port)
	if err := http.ListenAndServe(":"+port, withCORS(mux)); err != nil {
		log.Fatalf("listen: %v", err)
	}
}

// runWorker is the logical Order Service: it consumes order_created and
// writes the analytical projection into ClickHouse. At-least-once delivery is
// intentional; the MergeTree table tolerates duplicates via its order key.
func runWorker(ctx context.Context) {
	reader := newKafkaReader()
	ch := newClickHouseClient()
	defer reader.Close()

	log.Printf("order-worker consuming %s", topicOrderCreated)
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("fetch: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		var event SaleEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("decode event %q: %v", msg.Value, err)
			continue
		}
		if err := ch.InsertSale(ctx, event); err != nil {
			// No commit: Kafka will redeliver this message.
			log.Printf("insert sale %s: %v", event.OrderID, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit: %v", err)
		}
		log.Printf("sale %s -> clickhouse", event.OrderID)
	}
}

type clickHouseClient struct {
	baseURL  string
	user     string
	password string
	client   *http.Client
}

func (c clickHouseClient) do(ctx context.Context, query string, body io.Reader) ([]byte, error) {
	endpoint := c.baseURL + "/?query=" + url.QueryEscape(query)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, body)
	if err != nil {
		return nil, err
	}
	if c.user != "" {
		req.Header.Set("X-ClickHouse-User", c.user)
		req.Header.Set("X-ClickHouse-Key", c.password)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("clickhouse %d: %s", resp.StatusCode, string(payload))
	}
	return payload, nil
}

func (c clickHouseClient) InsertSale(ctx context.Context, event SaleEvent) error {
	row := map[string]any{
		"order_id":     event.OrderID,
		"product_id":   event.ProductID,
		"product_name": event.ProductName,
		"qty":          event.Qty,
		"total_cents":  event.TotalCents,
		"status":       event.Status,
		"created_at":   event.CreatedAt.UTC().Format("2006-01-02 15:04:05.000"),
	}
	encoded, err := json.Marshal(row)
	if err != nil {
		return err
	}
	_, err = c.do(ctx, "INSERT INTO sales FORMAT JSONEachRow", bytes.NewReader(append(encoded, '\n')))
	return err
}

func (c clickHouseClient) Sales(ctx context.Context) ([]SalePoint, error) {
	query := `SELECT toStartOfMinute(created_at) AS minute,
	                 count() AS orders,
	                 round(sum(total_cents) / 100.0, 2) AS revenue
	          FROM sales FINAL
	          WHERE status = 'paid'
	          GROUP BY minute
	          ORDER BY minute
	          SETTINGS output_format_json_quote_64bit_integers = 0
	          FORMAT JSON`
	payload, err := c.do(ctx, query, nil)
	if err != nil {
		return nil, err
	}
	var result struct {
		Data []SalePoint `json:"data"`
	}
	if err := json.Unmarshal(payload, &result); err != nil {
		return nil, err
	}
	return result.Data, nil
}

func (a *App) sales(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	points, err := a.ch.Sales(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	if points == nil {
		points = []SalePoint{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": points})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method != method {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return false
	}
	return true
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func serverError(w http.ResponseWriter, err error) {
	writeError(w, http.StatusInternalServerError, err.Error())
}

func newClickHouseClient() clickHouseClient {
	return clickHouseClient{
		baseURL:  strings.TrimRight(env("CLICKHOUSE_URL", "http://localhost:8123"), "/"),
		user:     env("CLICKHOUSE_USER", "default"),
		password: env("CLICKHOUSE_PASSWORD", ""),
		client:   &http.Client{Timeout: 10 * time.Second},
	}
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Idempotency-Key")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// pqUniqueViolation reports whether err is a Postgres UNIQUE constraint error.
func pqUniqueViolation(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && string(pqErr.Code) == "23505"
}
