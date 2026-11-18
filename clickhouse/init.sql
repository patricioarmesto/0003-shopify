CREATE DATABASE IF NOT EXISTS default;

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
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY order_id;
