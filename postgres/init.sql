CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    price_cents INTEGER NOT NULL CHECK (price_cents > 0),
    stock INTEGER NOT NULL CHECK (stock >= 0),
    emoji TEXT NOT NULL DEFAULT '📦'
);

CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key TEXT NOT NULL UNIQUE,
    user_id TEXT NOT NULL,
    product_id INTEGER NOT NULL REFERENCES products(id),
    product_name TEXT NOT NULL,
    qty INTEGER NOT NULL CHECK (qty > 0),
    total_cents INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'paid', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(id),
    status TEXT NOT NULL,
    response JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at DESC);

INSERT INTO products (id, name, description, price_cents, stock, emoji) VALUES
    (1,  'Steam Deck OLED',      'Consola portátil 512 GB',        56999, 10, '🎮'),
    (2,  'Teclado Mecánico',     'Switches marrones, 87 teclas',   14999, 24, '⌨️'),
    (3,  'Monitor 27" 4K',       'IPS, 144 Hz, USB-C',             42999,  8, '🖥️'),
    (4,  'Auriculares ANC',      'Cancelación activa de ruido',    29999, 15, '🎧'),
    (5,  'Silla Ergonómica',     'Respaldo mallado, lumbar',       79999,  6, '🪑'),
    (6,  'Webcam 4K',            'Sensor Sony, autofoco',          19999, 32, '📷'),
    (7,  'SSD NVMe 2 TB',        'PCIe 4.0, 7000 MB/s',           24999, 18, '💾'),
    (8,  'Mouse Inalámbrico',    '26K DPI, 60g',                   9999, 41, '🖱️'),
    (9,  'Dock Thunderbolt 4',   '8 puertos, 100W PD',            21999, 11, '🔌'),
    (10, 'Lámpara de Escritorio','Luz cálida/fría regulable',       6999, 27, '💡')
ON CONFLICT (id) DO NOTHING;

SELECT setval(pg_get_serial_sequence('products', 'id'),
              COALESCE((SELECT MAX(id) FROM products), 1));
