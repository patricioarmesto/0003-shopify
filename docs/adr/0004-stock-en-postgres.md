# ADR-004: Postgres como fuente de verdad del stock
- Ejercicio: 0003 de 1024 · Fecha: 2026-11-18 · Estado: Aceptado

## Contexto

El stock es el dato que decide si una orden se confirma. Debe ser durable, consistente y sobrevivir a reinicios de cualquier otro componente. Redis ya guarda reservas y una caché del catálogo; ClickHouse recibe eventos con retraso y agrega datos para gráficos. Ninguno de los dos puede ser la autoridad: la caché puede estar desactualizada y la analítica es asíncrona por diseño.

## Decisión

Postgres es la única fuente de verdad del stock. El descuento se ejecuta como una sola sentencia atómica:

```sql
UPDATE products SET stock = stock - $1
WHERE id = $2 AND stock >= $1
RETURNING name, price_cents;
```

Si la sentencia no devuelve filas, no hay stock y se responde `Sin stock`. La orden, la idempotency key y el descuento ocurren en la misma transacción. Redis solo cachea la proyección del catálogo por 30 segundos, y esa caché se invalida después de cada venta. ClickHouse nunca se consulta para decidir si se vende.

## Consecuencias (A favor / En contra)

A favor:
- No existe la secuencia `SELECT stock` → comprobar → `UPDATE`: la condición `stock >= qty` se evalúa dentro de la misma sentencia, así que 100 concurrentes sobre stock 10 nunca pueden confirmar más de 10 ventas. El pago mock rechaza alrededor del 5 % y libera su reserva, por lo que el número exacto de ventas varía entre corridas.
- No se necesitan locks de aplicación ni `SERIALIZABLE`: el lock de fila de Postgres basta.
- Stock, orden e idempotencia comparten transacción: no puede existir una orden sin su descuento correspondiente.
- El stock sobrevive a un reinicio de Redis o de ClickHouse.

En contra:
- Bajo carga extrema, todas las escrituras de stock confluyen en el mismo WAL y en las mismas filas; hay un límite físico de throughput por producto.
- La caché del catálogo puede mostrar stock viejo durante hasta 30 segundos en lecturas que no pasan por el checkout; se mitiga invalidándola tras cada venta.
- Cada checkout hace al menos tres consultas a Postgres (idempotencia, producto, descuento + inserts), lo que encarece el latency frente a un contador en memoria.

## Alternativas consideradas

- **Contador atómico `DECR` en Redis como fuente de verdad**: muy rápido, pero perdería durability ante un fallo de Redis y exigiría reconciliar con Postgres de todos modos.
- **Stock derivado de eventos en ClickHouse**: la analítica es eventual; una decisión de venta basada en ella aceptaría pedidos que ya no pueden cumplirse.
- **Reservas en Postgres con `SELECT FOR UPDATE`**: consistente, pero convierte cada carrito en una transacción durable; ver [ADR-001](0001-reserva-en-redis.md).

## Referencias

- Implementación: `backend/checkout.go` (descuento atómico y transacción).
- Caché e invalidación: `backend/product.go`.
- Esquema y semilla: `postgres/init.sql`.
- Prueba: [README.md § Cómo probar, "Prueba de carga"](../../README.md).
