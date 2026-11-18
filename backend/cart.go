package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

func cartKey(ref cartRef) string {
	return fmt.Sprintf("cart:%s:%d", ref.userID, ref.productID)
}

func reservationIndexKeys(productID int) (string, string) {
	return fmt.Sprintf("res_exp:%d", productID), fmt.Sprintf("res_qty:%d", productID)
}

// reserveResult names the small integers returned by the Lua script.
type reserveResult int

const (
	reserveOutOfStock reserveResult = iota
	reserveOK
	reserveDuplicate
)

func (r reserveResult) status() string {
	switch r {
	case reserveOK:
		return "reserved"
	case reserveDuplicate:
		return "already_reserved"
	default:
		return "out_of_stock"
	}
}

// reserveScript is atomic: purge expired holds, reject duplicates, compare the
// total reserved quantity with the real Postgres stock, then write the
// cart:{user_id}:{product_id} key with a 10 minute TTL.
var reserveScript = redis.NewScript(`
local zkey, hkey, ckey = KEYS[1], KEYS[2], KEYS[3]
local now, limit, qty, member, ttl = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), ARGV[4], tonumber(ARGV[5])

local expired = redis.call('ZRANGEBYSCORE', zkey, '-inf', now)
for _, m in ipairs(expired) do
  redis.call('ZREM', zkey, m)
  redis.call('HDEL', hkey, m)
end

if redis.call('EXISTS', ckey) == 1 then return 2 end

local total = 0
for _, v in ipairs(redis.call('HVALS', hkey)) do
  total = total + tonumber(v)
end
if total + qty > limit then return 0 end

redis.call('ZADD', zkey, now + ttl * 1000, member)
redis.call('HSET', hkey, member, qty)
redis.call('SET', ckey, qty, 'EX', ttl)
return 1
`)

var releaseScript = redis.NewScript(`
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
return 1
`)

func (a *App) reserveProduct(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
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

	ctx := r.Context()
	stock, err := a.getStock(ctx, req.ProductID)
	if err == sql.ErrNoRows {
		writeError(w, http.StatusNotFound, "product not found")
		return
	}
	if err != nil {
		serverError(w, err)
		return
	}

	ref := req.ref()
	zkey, hkey := reservationIndexKeys(req.ProductID)
	result, err := reserveScript.Run(ctx, a.rdb,
		[]string{zkey, hkey, cartKey(ref)},
		time.Now().UnixMilli(), stock, req.Qty, req.UserID, int(reservationTTL.Seconds()),
	).Int()
	if err != nil {
		serverError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":      reserveResult(result).status(),
		"key":         cartKey(ref),
		"ttl_seconds": int(reservationTTL.Seconds()),
		"stock":       stock,
	})
}

func (a *App) releaseReservation(ctx context.Context, ref cartRef) error {
	zkey, hkey := reservationIndexKeys(ref.productID)
	return releaseScript.Run(ctx, a.rdb,
		[]string{cartKey(ref), zkey, hkey}, ref.userID).Err()
}
