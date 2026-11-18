# ADR-001: Reserva en Redis en lugar de Postgres
- Ejercicio: 0003 de 1024 · Fecha: 2026-11-18 · Estado: Aceptado

## Contexto

El checkout debe sostener picos de tráfico de Black Friday sin vender más unidades de las existentes. Cada carrito necesita una reserva temporal de 10 minutos que se libere automáticamente si el usuario no paga. La reserva es un estado efímero: expira, se reemplaza y nunca debe aparecer en un reporte financiero. Postgres ya almacena productos, órdenes e idempotency keys, y cada escritura compite por el mismo WAL y los mismos locks.

## Decisión

Las reservas viven en Redis bajo la clave `cart:{user_id}:{product_id}` con TTL de 600 segundos. Un script Lua atómico purga las reservas vencidas de un índice ordenado (`res_exp:{product_id}`), suma las cantidades de un hash (`res_qty:{product_id}`), compara ese total contra el stock vigente leído de Postgres y escribe la clave del carrito solo si hay cupo. El pago, exitoso o fallido, consume o libera la reserva con otro script Lua. Postgres no registra reservas.

## Consecuencias (A favor / En contra)

A favor:
- Las reservas expiran solas por TTL; no hace falta un job de limpieza.
- Expirar, contar y reservar ocurre en una sola operación atómica en Redis, sin locks de aplicación.
- El WAL de Postgres recibe escrituras solo de decisiones definitivas (órdenes y stock), no de carritos abandonados.
- El límite de reservas se toma del stock real de Postgres, así que Redis nunca inventa inventario.

En contra:
- Redis y Postgres pueden divergir durante una ventana corta: si Redis cae se pierden las reservas y el checkout las rechaza hasta que se renueven.
- Si Postgres cae después de iniciada una reserva, el cupo quedó tomado en Redis sin stock que lo respalde hasta que expire.
- La lógica vive en un script Lua, menos familiar para quien solo lee Go.

## Alternativas consideradas

- **Reserva como fila en Postgres con `expires_at`**: más consistente y consultable, pero convierte cada carrito en una escritura durable y obliga a un worker de expiración. Desplaza el cuello de botella justo al lugar que queremos proteger.
- **Solo `UPDATE ... WHERE stock >= qty` en el checkout, sin reservas**: evita Redis, pero el usuario descubre la falta de stock únicamente al pagar y no existe noción de "carrito con hold". No cubre el requisito de reserva de 10 minutos.
- **TTL nativo de Postgres con particionado por tiempo**: complejo de operar y sin la atomicidad de un script Lua para el conteo de cupo.

## Referencias

- Implementación: `backend/cart.go` (`reserveScript`, `releaseScript`).
- Flujo: [README.md § Flujo, pasos 2 y 4](../../README.md).
- Decisión complementaria: [ADR-004 — Postgres como fuente de verdad del stock](0004-stock-en-postgres.md).
