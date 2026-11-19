CREATE DATABASE IF NOT EXISTS default;

-- sales es la proyección analítica: se escribe una vez por evento y se lee
-- agrupada por minuto para el gráfico. Nunca se consulta para decidir ventas.
CREATE TABLE IF NOT EXISTS default.sales
(
    order_id     String,
    product_id   Int32,
    product_name String,
    qty          Int32,
    total_cents  Int32,
    status       String,
    created_at   DateTime64(3)
)
-- ReplacingMergeTree colapsa las filas con el mismo order_id, eligiendo la de
-- created_at más reciente. Kafka entrega at-least-once, así que un redelivery
-- insertaría dos veces la misma orden; sin esto el gráfico contaría doble.
-- ORDER BY en MergeTree sólo ordena el almacenamiento: NO deduplica. Por eso
-- la consulta del api usa FINAL (ver backend/main.go).
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY order_id;
