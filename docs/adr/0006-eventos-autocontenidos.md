# ADR-006: Eventos autocontenidos y un partition por topic
- Ejercicio: 0003 de 1024 · Fecha: 2026-11-18 · Estado: Aceptado

## Contexto

El checkout publica en Kafka y el `order-worker` alimenta ClickHouse con esos eventos. Dos decisiones quedan invisibles al leer el código: qué información viaja en el mensaje y cuántas particiones tiene el topic. Un lector razonable podría preferir publicar sólo el `order_id` y que el consumidor consulte el dato fresco en Postgres, o nombrar los topics `orders` y `payments` en lugar de `order_created` y `payment_failed`. Además, la publicación ocurre después del `COMMIT`, así que con checkouts concurrentes el orden en Kafka no coincide necesariamente con el orden de confirmación en Postgres. Esa diferencia es inofensiva sólo por una propiedad del evento que conviene dejar escrita.

## Decisión

`order_created` lleva un snapshot de siete campos —`order_id`, `product_id`, `product_name`, `qty`, `total_cents`, `status` y `created_at`— con el `order_id` como key. `payment_failed` lleva `idempotency_key`, `user_id`, `product_id`, `qty`, `total_cents`, `reason` y `occurred_at`, con la idempotency key como key. El worker sólo decodifica el JSON e inserta en ClickHouse: no conoce Postgres ni abre conexiones contra él.

Los topics se nombran por el hecho en pasado, no por la entidad. Hay un solo hecho por topic, así que el nombre ya actúa como filtro. No existe un topic `payments` porque el pago exitoso no se publica: queda registrado como `status: paid` dentro de `order_created`. Un topic `payments` con exclusivamente rechazos sería engañoso.

`kafka-init` crea ambos topics explícitamente con una partición, y el worker consume de forma secuencial confirmando el offset recién después de insertar en ClickHouse. El `created_at` viaja dentro del evento y el gráfico agrupa por `toStartOfMinute(created_at)`, no por offset. Por eso el desorden entre el `COMMIT` y el `publish` no altera ningún agregado: la analítica es conmutativa.

## Consecuencias (A favor / En contra)

A favor:
- El worker es un módulo profundo y reemplazable: entra un JSON, sale un INSERT, y no depende de un segundo almacén.
- Los tests del worker usan un fixture JSON, sin sembrar Postgres.
- `product_name` es el de la venta, no el nombre vigente en `products` al momento de consultar. Si mañana se renombra un producto, el gráfico sigue mostrando lo comprado.
- El payload son ~200 bytes y con una partición el orden por topic queda garantizado sin coordinación adicional.
- La key ya es el `order_id`, así que al escalar a varias particiones se preserva el orden por orden.

En contra:
- El esquema del evento está duplicado entre productor y consumidor: cambiar un campo exige tocar ambos y coordinar el despliegue.
- Si mañana hay más consumidores con necesidades distintas, cada uno deberá reconstruir lo que necesite a partir de un payload que quizá no traiga ese dato.
- Una partición por topic limita el throughput del worker. Escalar exigirá revisar este ADR.
- El desorden `COMMIT` → `publish` seguiría siendo un problema si el worker empezara a calcular estado derivado donde el último evento gana, como stock restante o último estado por orden.

## Alternativas consideradas

- **Event notification: publicar sólo `order_id` y consultar `orders`**: achica el productor pero agranda la interfaz del worker, que pasaría a conocer el esquema de Postgres, el DSN, el pool y la política de reintento. Además, buscar el nombre en `products` devolvería el valor actual, no el de la venta. Hoy hay un solo consumidor con una necesidad fija, así que el snapshot gana.
- **Topics por entidad (`orders`, `payments`) con un campo `type`**: tiene sentido con event sourcing o CDC, donde conviven `order_created`, `order_shipped`, `order_cancelled`. Acá hay un hecho por entidad y el pago exitoso ni siquiera se publica; agregar el discriminador sería generalidad especulativa.
- **Varias particiones desde el principio con versión en el payload**: útil para throughput y consumidores con estado por entidad, pero este ejercicio tiene un consumidor y agregados conmutativos. La complejidad no se justifica todavía.

## Referencias

- Estructura del evento: `backend/main.go` (`SaleEvent`) y `backend/checkout.go` (`publish`).
- Consumo secuencial y commit manual: `backend/main.go` (`newKafkaReader`, `runWorker`).
- Creación de topics: `docker-compose.yml` (`kafka-init`).
- Agregado por `created_at`: `clickhouse/init.sql` y `GET /api/analytics/sales`.
- Decisión complementaria: [ADR-005 — Transactional Outbox fuera de alcance](0005-sin-transactional-outbox.md).
