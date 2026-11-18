# ADR-002: Idempotency-Key obligatoria en checkout
- Ejercicio: 0003 de 1024 · Fecha: 2026-11-18 · Estado: Aceptado

## Contexto

Un doble click, un reintento de red o un usuario que recarga la página pueden producir dos peticiones de checkout idénticas. Si cada petición llama al Payment Service, el cliente ve dos cargos y el sistema crea dos órdenes. El problema no se resuelve con deshabilitar el botón: el navegador puede reenviar la petición igualmente. La propiedad de "cobrar una sola vez" debe estar en el servidor.

## Decisión

Toda petición a `POST /api/checkout` debe incluir el header `Idempotency-Key`; sin él se responde `400`. El API adquiere un mutex en memoria por clave, consulta la tabla `idempotency_keys` de Postgres y, si la clave ya fue completada, devuelve exactamente la respuesta JSON almacenada con el header `X-Idempotent-Replay: true`, sin invocar al pago. Si no existe, ejecuta el flujo y persiste clave, orden y respuesta en la misma transacción. La columna `key` es PRIMARY KEY y `orders.idempotency_key` es UNIQUE, de modo que una segunda réplica que no comparta mutex recibe una violación de unicidad y devuelve la orden ya creada.

## Consecuencias (A favor / En contra)

A favor:
- El doble click es indistinguible del reintento legítimo: ambos producen una sola orden y un solo cargo.
- La respuesta repetida es byte-idéntica a la original, incluido el `order_id`, lo que simplifica la reconciliación del cliente.
- La restricción UNIQUE en Postgres protege incluso si mañana se escala el API a varias réplicas.
- La clave viaja en un header, no en el body, así que queda visible en la UI y en las herramientas de prueba.

En contra:
- Cada checkout agrega una consulta previa y una escritura adicional en Postgres.
- El mutex por clave crece con el número de claves distintas; en un ejercicio de 100 usuarios es insignificante, en producción exigiría evicción o un lock distribuido.
- El cliente debe generar y conservar la clave; una clave nueva para el mismo carrito es, por diseño, un cobro nuevo.
- Solo se registran claves completadas: un pago fallido con la misma clave puede reintentarse.

## Alternativas consideradas

- **Confiar en el idempotency key de Stripe**: queda descartado porque el pago es un mock (ver ADR-003).
- **Deduplicar por hash del body**: dos compras legítimas del mismo producto con la misma cantidad serían confundidas; además el body no identifica al comprador de forma única.
- **Lock distribuido en Redis**: resolvería las múltiples réplicas, pero introduce otro sistema crítico en la ruta de pago. La UNIQUE constraint de Postgres cubre el caso con menos piezas.

## Referencias

- Implementación: `backend/checkout.go` (`keyLocks`, tabla `idempotency_keys`).
- Esquema: `postgres/init.sql`.
- Prueba en la UI: botón **Pagar (doble click)** en `frontend/src/App.tsx`.
- Flujo: [README.md § Flujo, paso 3](../../README.md).
