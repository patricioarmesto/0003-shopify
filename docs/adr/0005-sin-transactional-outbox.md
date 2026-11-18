# ADR-005: Transactional Outbox fuera de alcance
- Ejercicio: 0003 de 1024 · Fecha: 2026-11-18 · Estado: Aceptado

## Contexto

El checkout hace dos cosas separadas: confirma la orden en Postgres y publica `order_created` en Kafka. Entre el `COMMIT` y la llamada al productor hay una ventana de fallo. Si el proceso muere en ese instante, Postgres queda con una orden pagada que nunca llegó a ClickHouse, y el gráfico de ventas subestima la facturación. El Transactional Outbox Pattern —insertar el evento en una tabla dentro de la misma transacción y publicarlo con un relay— cierra esa ventana.

## Decisión

No implementar Transactional Outbox en este ejercicio. La publicación ocurre después del commit, con `log.Printf` en caso de error. El worker de órdenes consume con at-least-once y confirma el offset solo después de insertar en ClickHouse. La tabla `sales` usa `ReplacingMergeTree(created_at)` con `ORDER BY order_id` y el gráfico consulta con `FINAL`, de modo que un redelivery colapsa en la misma fila y no altera el agregado. `ORDER BY` en `MergeTree` sólo ordena el almacenamiento: por sí solo no deduplica.

## Consecuencias (A favor / En contra)

A favor:
- El flujo de checkout se lee de arriba abajo: pagar, descontar, insertar, publicar. No hay tabla de eventos ni relay que explicar.
- Hay una sola transacción de Postgres, en lugar de dos fases con reintento.
- Mantiene el ejercicio en el nivel semana 3: el fallo se nombra y se entiende, sin implementar su solución.

En contra:
- Existe una ventana real de pérdida del evento de analítica si el proceso cae tras el commit.
- Un error de Kafka deja la orden pagada sin fila en ClickHouse; no hay reintento automático desde Postgres.
- El comportamiento no es apto para producción y no debe copiarse a un sistema de pagos real.

## Alternativas consideradas

- **Transactional Outbox con relay polling**: consistente y probado en industria, pero agrega tabla, worker de publicación y limpieza; desplaza el foco del checkout a la infraestructura de mensajería.
- **CDC sobre el WAL de Postgres (Debezium)**: elimina el código de publicación, pero incorpora Kafka Connect y despliega una topología completa solo para un gráfico.
- **Doble escritura síncrona a ClickHouse**: quita Kafka del camino, rompe el requisito de eventos y hace que un fallo de analítica bloquee el checkout.

## Referencias

- Publicación y manejo de error: `backend/checkout.go` (`publish`).
- Worker y confirmación de offset: `backend/main.go` (`runWorker`).
- Esquema de ClickHouse: `clickhouse/init.sql`.
- Alcance explícito: [README.md § Qué NO implementamos](../../README.md).
