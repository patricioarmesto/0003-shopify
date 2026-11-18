package main

import (
	"fmt"
	"math/rand"
	"time"
)

// PaymentService is the Mock Payment Service. Fixed latency and a fixed
// failure rate make the pending / paid / failed states reproducible.
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
