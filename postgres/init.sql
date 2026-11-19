-- init.sql corre una sola vez, cuando el volumen de Postgres está vacío.
-- Define la fuente de verdad del ejercicio: stock, órdenes e idempotencia.
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    price_cents INTEGER NOT NULL CHECK (price_cents > 0),
    -- El CHECK impide stock negativo incluso si alguien escribe a mano.
    stock INTEGER NOT NULL CHECK (stock >= 0),
    emoji TEXT NOT NULL DEFAULT '📦'
);

CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- El UNIQUE es el respaldo definitivo contra el doble click: aunque dos
    -- réplicas pasen el mutex local, Postgres permite una sola orden por clave.
    idempotency_key TEXT NOT NULL UNIQUE,
    user_id TEXT NOT NULL,
    product_id INTEGER NOT NULL REFERENCES products(id),
    -- Nombre denormalizado: la orden debe recordar qué se compró aunque luego
    -- cambie el catálogo. Evita un JOIN para mostrar el panel de órdenes.
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

-- El panel pide las últimas 100 órdenes ordenadas por fecha; sin este índice
-- Postgres ordenaría la tabla completa en cada polling de 1 segundo.
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at DESC);

-- El producto 1 arranca con stock 10 a propósito: es el que usa la prueba de
-- carga de 100 usuarios. Los demás tienen stock distinto para que el catálogo
-- no parezca un fixture uniforme.
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

-- Los IDs se insertaron explícitamente, así que hay que mover el secuenciador;
-- si no, el próximo INSERT chocaría con el producto 1.
SELECT setval(pg_get_serial_sequence('products', 'id'),
              COALESCE((SELECT MAX(id) FROM products), 1));
