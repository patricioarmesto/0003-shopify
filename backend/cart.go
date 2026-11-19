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

// cart.go implementa la primera línea de defensa del Black Friday: una reserva
// breve en Redis que evita que cien usuarios peleen por el mismo stock antes de
// llegar a Postgres. El stock real sigue viviendo en Postgres; Redis sólo decide
// quién tiene derecho a intentar el checkout durante diez minutos.

// cartKey respeta literalmente el formato cart:{user_id}:{product_id} del
// enunciado. Es la clave que el estudiante puede inspeccionar con redis-cli.
func cartKey(ref cartRef) string {
	return fmt.Sprintf("cart:%s:%d", ref.userID, ref.productID)
}

// reservationIndexKeys devuelve el ZSET de vencimientos y el HASH de cantidades
// por producto. Hacen falta porque un TTL por clave no permite sumar cuánto
// está reservado: el ZSET guarda cuándo expira cada usuario y el HASH cuántas
// unidades tiene, para poder purgar y recalcular el total en un solo paso.
func reservationIndexKeys(productID int) (string, string) {
	return fmt.Sprintf("res_exp:%d", productID), fmt.Sprintf("res_qty:%d", productID)
}

// reserveResult da nombre a los enteros que devuelve el script Lua. Sin este
// tipo, el código tendría «case 1» y «case 2» sin decir qué significan.
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

// reserveScript corre completo dentro de Redis: purga reservas vencidas,
// rechaza duplicados, compara el total reservado con el stock que informó
// Postgres y recién entonces escribe la clave con TTL de diez minutos.
//
// ¿Por qué Lua y no un GET + SET desde Go? Porque dos requests concurrentes
// podrían leer el mismo total y ambos decidir que hay lugar. Al ejecutar todo
// como un único script, el segundo ve el efecto del primero. Es la misma idea
// del UPDATE atómico de Postgres, aplicada a la reserva.
var reserveScript = redis.NewScript(`
local zkey, hkey, ckey = KEYS[1], KEYS[2], KEYS[3]
local now, limit, qty, member, ttl = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), ARGV[4], tonumber(ARGV[5])

-- Una reserva vencida queda si el proceso murió antes de liberarla.
local expired = redis.call('ZRANGEBYSCORE', zkey, '-inf', now)
for _, m in ipairs(expired) do
  redis.call('ZREM', zkey, m)
  redis.call('HDEL', hkey, m)
end

-- Un doble click del mismo usuario no debe consumir dos unidades.
if redis.call('EXISTS', ckey) == 1 then return 2 end

local total = 0
for _, v in ipairs(redis.call('HVALS', hkey)) do
  total = total + tonumber(v)
end
-- limit viene de Postgres: Redis nunca inventa stock.
if total + qty > limit then return 0 end

redis.call('ZADD', zkey, now + ttl * 1000, member)
redis.call('HSET', hkey, member, qty)
redis.call('SET', ckey, qty, 'EX', ttl)
return 1
`)

// releaseScript borra la clave y también sus dos entradas del índice. Si sólo
// se borrara la clave, el índice seguiría contando esa reserva como ocupada
// hasta que venciera: stock bloqueado sin nadie que lo compre.
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

	// El techo de la reserva se lee de Postgres, la única fuente de verdad.
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
