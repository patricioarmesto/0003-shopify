# ADR-003: Payment Service mock en lugar de Stripe
- Ejercicio: 0003 de 1024 · Fecha: 2026-11-18 · Estado: Aceptado

## Contexto

El objetivo del ejercicio es entender concurrencia, reservas, idempotencia y analítica de eventos. Un proveedor de pagos real introduce credenciales, webhooks, modo sandbox, latencia de red variable y retries del proveedor. Además, para demostrar el estado `failed` haría falta provocar un rechazo de forma controlada, algo que Stripe no garantiza bajo demanda. El aula necesita un componente reproducible y que funcione sin acceso a internet.

## Decisión

`PaymentService.Charge` duerme 500 ms y devuelve un fallo con probabilidad 0.05 usando `math/rand`. El éxito incluye un `transaction_id` sintético; el fallo incluye un motivo estable. El mock es sincrónico dentro del request de checkout para que la UI pueda mostrar el estado `pending` mientras transcurre la latencia. No hay servidor de pagos separado: es una unidad dentro del API con una interfaz lo bastante pequeña para ser reemplazada.

## Consecuencias (A favor / En contra)

A favor:
- `docker compose up` funciona offline, sin claves API ni cuentas de terceros.
- Latencia y tasa de fallo son constantes: el estudiante ve el mismo comportamiento en cada corrida.
- Los tres estados del panel (`pending`, `paid`, `failed`) aparecen de forma natural, sin forzarlos a mano.
- La interfaz `Charge(key, amount) PaymentResult` es el punto de sustitución por un adaptador real.

En contra:
- No valida tarjetas, no calcula comisiones y no expone dispute/chargeback; nada de eso se enseña aquí.
- El generador aleatorio global no es criptográfico, lo cual es correcto para un mock e incorrecto para un ID de transacción real.
- Los 500 ms fijos son artificiales: en producción la latencia tendría colas y percentiles, no un valor único.
- Un reintento con clave distinta puede cobrar otra vez, porque la decisión de idempotencia vive en el checkout, no en el pago.

## Alternativas consideradas

- **Stripe en modo test**: fiel a la industria, pero requiere secretos y webhooks, y desplaza la atención del sistema distribuido hacia un SDK externo.
- **Servicio de pago mock separado en Docker**: más realista en forma, pero agrega un contenedor y una llamada de red que no aportan a los conceptos del ejercicio.
- **Pago siempre exitoso**: simplifica, pero elimina el evento `payment_failed` y la liberación de la reserva, que son parte del requisito.

## Referencias

- Implementación: `backend/payment.go`.
- Publicación de `payment_failed`: `backend/checkout.go`.
- Alcance: [README.md § Qué NO implementamos](../../README.md).
- Decisión relacionada: [ADR-002 — Idempotency Key obligatoria](0002-idempotency-key.md).
