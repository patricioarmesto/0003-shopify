package main

import (
	"fmt"
	"math/rand"
	"time"
)

// payment.go simula al proveedor de pagos sin red, credenciales ni webhooks.
// La latencia fija de 500 ms existe para que el estado pending sea visible en
// el panel: si el cargo fuera instantáneo, el estudiante nunca lo vería. El 5 %
// de rechazos hace observable el estado failed y, sobre todo, permite ver cómo
// se libera una reserva cuando el pago no llega a concretarse.

// PaymentService es el Mock Payment Service. Latencia y tasa de fallo fijas
// hacen reproducibles los estados pending / paid / failed.
type PaymentService struct {
	latency     time.Duration
	failureRate float64
}

type PaymentResult struct {
	OK            bool   `json:"ok"`
	TransactionID string `json:"transaction_id,omitempty"`
	FailureReason string `json:"failure_reason,omitempty"`
}

func NewPaymentService() *PaymentService {
	return &PaymentService{
		latency:     500 * time.Millisecond,
		failureRate: 0.05,
	}
}

// Charge ignora la clave y el monto a propósito: hoy cualquier cobro tarda lo
// mismo y falla con la misma probabilidad. Están en la firma para que reemplazar
// el mock por un proveedor real no obligue a cambiar el checkout (ver ADR-003).
func (p *PaymentService) Charge(idempotencyKey string, amountCents int) PaymentResult {
	time.Sleep(p.latency)
	if rand.Float64() < p.failureRate {
		return PaymentResult{
			OK:            false,
			FailureReason: "mock_declined: issuer temporarily unavailable",
		}
	}
	return PaymentResult{
		OK:            true,
		TransactionID: fmt.Sprintf("txn_%d_%d", time.Now().UnixNano(), rand.Int63n(1_000_000)),
	}
}
