# ¿Cómo funciona Shopify?

Ejercicio 0003 de 1024, Nivel: Avanzado, Stack: Go + React + Postgres + Redis + Kafka + ClickHouse

## Qué resuelve

Un checkout de e-commerce que no se rompe bajo carga de Black Friday. Tres garantías concretas:

1. **No vender stock que no hay.** El descuento de stock es un `UPDATE ... WHERE stock >= qty` atómico en Postgres, la fuente de verdad. No existe camino que permita confirmar una orden sin stock real.
2. **No cobrar dos veces por doble click.** Toda petición de checkout exige un `Idempotency-Key`. El API serializa por clave, persiste la clave en Postgres y devuelve la orden original ante un reintento, sin llamar al pago otra vez.
3. **Mostrar ventas en vivo.** Cada orden confirmada publica `order_created` a Kafka; un worker consume el evento, escribe en ClickHouse y el frontend grafica la facturación con Recharts.

Alrededor de esas garantías hay reservas de 10 minutos en Redis, un pago mock con latencia y fallos aleatorios, y un panel de órdenes con estados `pending`, `paid` y `failed`.

## Cómo correr

Un solo comando:

```bash
docker compose up --build
```

Esperá a que `backend` y `frontend` estén arriba (los healthchecks de Postgres, Redis, Kafka y ClickHouse gobiernan el arranque; `order-worker` se inicia apenas Kafka y ClickHouse están listos). Luego abrí:

- Frontend: http://localhost:5175
- API: http://localhost:8081/api/products

Los puertos de host están desplazados (5175, 8081, 5434, 6380, 8124, 9001) para poder correr varios ejercicios del curso al mismo tiempo. Dentro de la red de Compose los servicios usan los puertos estándar (`backend:8080`, `postgres:5432`, `redis:6379`, `kafka:9092`, `clickhouse:8123`).

Para reiniciar desde cero, incluyendo volúmenes:

```bash
docker compose down -v && docker compose up --build
```

## Arquitectura (diagrama en texto)

```
                          ┌────────────────────┐
   Navegador (React) ───▶ │ frontend (nginx)   │  :5173
                          │ Vite + Tailwind    │
                          └─────────┬──────────┘
                                    │ /api/*
                          ┌─────────▼──────────┐
                          │ API Go             │  :8080
                          │ products / cart /  │
                          │ checkout / payment │
                          └──┬──────┬──────┬───┘
          ┌──────────────────┘      │      └─────────────────┐
          ▼                         ▼                        ▼
 ┌─────────────────┐      ┌──────────────────┐     ┌──────────────────┐
 │ Postgres        │      │ Redis            │     │ Kafka (KRaft)    │
 │ products / stock│      │ caché productos  │     │ order_created    │
 │ orders / idem...│      │ reservas 10 min  │     │ payment_failed   │
 └─────────────────┘      └──────────────────┘     └────────┬─────────┘
          │                                                │ order_created
          │ GET /api/orders (polling)                       ▼
          │                                      ┌────────────────────┐
          └──────────────────────────────────────▶│ order-worker (Go)  │
                                                  │ consume → ClickHouse│
                                                  └─────────┬──────────┘
                                                            ▼
                                                  ┌────────────────────┐
                                                  │ ClickHouse         │
                                                  │ sales / gráfico    │
                                                  └────────────────────┘
```

Separación de responsabilidades: el API es el único que escribe en Postgres y Redis; el pago es un mock invocado en el request de checkout; Kafka desacopla la confirmación de pedido de la analítica; el worker es el único que escribe en ClickHouse. El panel en vivo consulta `GET /api/orders` por polling (1 s), no un WebSocket, para mantener el ejercicio legible.

## Flujo

1. `GET /api/products` → el API busca `products:list` en Redis; si no está, consulta Postgres, guarda el JSON con TTL de 30 s y responde los 10 productos con stock real.
2. `POST /api/cart/reserve` con `{ "product_id": 1, "qty": 1 }` → un script Lua atómico en Redis expira reservas viejas, verifica el cupo disponible con el stock actual de Postgres y escribe `cart:{user_id}:{product_id}` con TTL de 600 s. Devuelve `reserved`, `already_reserved` o `out_of_stock`.
3. `POST /api/checkout` con `{ "product_id": 1, "qty": 1, "user_id": "u-42" }` y header `Idempotency-Key: abc-123`:
   - Se adquiere un mutex en memoria por clave y se consulta `idempotency_keys` en Postgres. Si ya existe, se devuelve la respuesta stored, sin cobrar.
   - Se valida que exista la reserva activa.
   - Se llama al Payment Service mock (500 ms, 5 % de fallos).
4. **Pago OK** → en una sola transacción de Postgres: `UPDATE products SET stock = stock - qty WHERE id = ... AND stock >= qty`, `INSERT INTO orders` con `idempotency_key` UNIQUE e `INSERT INTO idempotency_keys`. Se libera la reserva, se publica `order_created` a Kafka y se responde `201 paid`.
5. **Pago FAIL** → se libera la reserva en Redis, no se crea orden, se publica `payment_failed` a Kafka y se responde `402 payment_failed`.
6. El `order-worker` consume `order_created`, guarda la fila en `sales` (ClickHouse) y confirma el offset. `GET /api/analytics/sales` alimenta el gráfico de Recharts.
7. El botón **Simular 100 usuarios** lanza 100 pares reserve+checkout en paralelo sobre el producto con stock 10. Cada usuario tiene un `user_id` distinto y una `Idempotency-Key` distinta; el contador muestra cuántos compraron, cuántos vieron "Sin stock" y cuántos fallaron el pago.

## Stack

Solo tecnologías presentes en `docker-compose.yml`.

| Tecnología | Rol | Servicio en Compose |
| --- | --- | --- |
| Go | API HTTP, reservas, pago mock y consumer de Kafka | `backend`, `order-worker` |
| React | UI: productos, checkout, panel de órdenes y gráfico | `frontend` |
| Vite | Bundler y servidor de build del frontend | `frontend` |
| Tailwind CSS | Estilos utilitarios | `frontend` |
| PostgreSQL | Stock, órdenes e idempotency keys (fuente de verdad) | `postgres` |
| Redis | Caché de productos y reservas con TTL de 10 min | `redis` |
| Apache Kafka | Eventos `order_created` y `payment_failed` (modo KRaft) | `kafka` |
| ClickHouse | Analítica de ventas en vivo | `clickhouse` |
| Docker | Build multi-stage de API y frontend | build de `backend` / `frontend` |
| Docker Compose | Orquestación de un solo comando | `docker-compose.yml` |

## Estructura del proyecto

```text
.
├── docker-compose.yml      # orquestación de los ocho servicios
├── postgres/init.sql       # esquema y semilla (producto 1 con stock 10)
├── clickhouse/init.sql     # tabla sales con ReplacingMergeTree
├── clickhouse/users.xml    # usuario app accesible desde la red de Compose
├── backend/                # API HTTP, reservas, pago mock y worker de órdenes
├── frontend/               # React + Vite + Tailwind servido por nginx
└── docs/adr/               # las seis decisiones arquitectónicas
```

## Decisiones clave

- **Stock atómico, no `SELECT` + `UPDATE`.** El descuento usa `WHERE stock >= qty RETURNING ...` en una sola sentencia; 100 concurrentes sobre stock 10 producen como máximo 10 reservas y el resto recibe "Sin stock", sin locks de aplicación. Algunas de esas reservas pueden terminar en pago rechazado y liberarse, así que el reparto final entre éxitos y rechazos no es fijo.
- **Reservas en Redis con script Lua.** Expirar reservas viejas, sumar cantidades reservadas y escribir la clave `cart:{user_id}:{product_id}` ocurre en una sola operación atómica. Ver [ADR-0001](docs/adr/0001-reserva-en-redis.md).
- **Idempotencia persistida.** Mutex por clave dentro del proceso más `UNIQUE (idempotency_key)` en Postgres. El mutex da buen UX al doble click; la restricción única protege si algún día hay varias réplicas. Ver [ADR-0002](docs/adr/0002-idempotency-key.md).
- **Pago simulado, no Stripe.** Latencia fija de 500 ms y 5 % de fallos hacen reproducibles el `pending`, el `paid` y el `failed`. Ver [ADR-0003](docs/adr/0003-pago-mock.md).
- **Postgres manda en stock; ClickHouse solo lee eventos.** Nunca se consulta ClickHouse para decidir si se vende. Ver [ADR-0004](docs/adr/0004-stock-en-postgres.md).
- **Sin transactional outbox.** La publicación a Kafka ocurre después del commit; en caso de crash entre ambas, el demo puede perder un evento de analítica. Ver [ADR-0005](docs/adr/0005-sin-transactional-outbox.md).
- **Eventos autocontenidos y un partition por topic.** El evento lleva la foto de la venta y su `created_at`; el worker no consulta Postgres y el desorden entre `COMMIT` y `publish` no altera el gráfico. Ver [ADR-0006](docs/adr/0006-eventos-autocontenidos.md).

## Alcance y decisiones de diseño

El API corre en una réplica y el worker en otra; la consistencia se apoya en restricciones de Postgres y en scripts Lua de Redis, no en coordinación distribuida. El pago mock es sincrónico dentro del request para que el estudiante vea el estado `pending` en el panel. El panel de órdenes usa polling de 1 s y el gráfico consulta ClickHouse bajo demanda. Todo lo que no está en esta lista queda **fuera de alcance a propósito para mantener el ejercicio legible.**

## Qué NO implementamos

- Stripe ni ningún proveedor de pago real; el cargo es un mock determinista en latencia y probabilidad de fallo.
- Transactional Outbox Pattern para publicar eventos dentro de la transacción de Postgres.
- Dead Letter Topics (DLT) para mensajes fallidos de Kafka.
- Exactly-Once Semantics entre Kafka y ClickHouse; el worker usa at-least-once y la tabla `ReplacingMergeTree` colapsa las reentregas por `order_id`.
- Monitoreo de producción (métricas, trazas, alertas, dashboards).

Estos cinco puntos están **fuera de alcance a propósito para mantener el ejercicio legible.**

## Cómo probar

**1. Lista de productos con stock desde Postgres y caché en Redis:**

```bash
curl -s http://localhost:8081/api/products | jq '.[0]'
redis-cli -h localhost -p 6380 TTL products:list
```

**2. Reserva de 10 minutos:**

```bash
curl -s -X POST http://localhost:8081/api/cart/reserve \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"u-1","product_id":1,"qty":1}'
redis-cli -h localhost -p 6380 TTL cart:u-1:1
```

**3. Checkout normal y doble click (misma Idempotency-Key):**

```bash
curl -s -X POST http://localhost:8081/api/checkout \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: abc-123' \
  -d '{"user_id":"u-1","product_id":1,"qty":1}'

curl -s -X POST http://localhost:8081/api/checkout \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: abc-123' \
  -d '{"user_id":"u-1","product_id":1,"qty":1}'
```

La segunda respuesta devuelve la misma `order_id` y el stock se descuenta una sola vez. En la UI, cargá la clave `abc-123` y apretá **Pagar (doble click)** dos veces rápido.

**4. Prueba de carga desde la UI:**

Apretá **Simular 100 usuarios**. El panel muestra `paid`, `sin stock` y `payment failed`; la suma es 100. El producto elegido arranca con stock 10. Para repetir la prueba sin reconstruir el stack, apretá **Reset stock** (equivale a `POST /api/dev/reset`: restaura el stock del producto 1 y limpia sus reservas en Redis; las órdenes ya creadas se conservan). Es un endpoint de desarrollo: el backend sólo lo registra cuando arranca con `ENABLE_DEV_ENDPOINTS=true`, valor ya seteado en `docker-compose.yml`.

**5. Órdenes y analítica:**

```bash
curl -s http://localhost:8081/api/orders | jq '.[0]'
curl -s http://localhost:8081/api/analytics/sales | jq '.data[0]'
```

También podés consultar ClickHouse directo:

```bash
docker compose exec clickhouse clickhouse-client --query \
  'SELECT count(), sum(total_cents)/100 FROM sales WHERE status = '\''paid'\'''
```

## ADRs

- [ADR-0001 — Reserva en Redis](docs/adr/0001-reserva-en-redis.md)
- [ADR-0002 — Idempotency Key obligatoria](docs/adr/0002-idempotency-key.md)
- [ADR-0003 — Pago mock en vez de Stripe](docs/adr/0003-pago-mock.md)
- [ADR-0004 — Postgres como fuente de verdad del stock](docs/adr/0004-stock-en-postgres.md)
- [ADR-0005 — Sin Transactional Outbox](docs/adr/0005-sin-transactional-outbox.md)
- [ADR-0006 — Eventos autocontenidos y un partition por topic](docs/adr/0006-eventos-autocontenidos.md)
