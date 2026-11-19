import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  Area,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

// App.tsx es toda la UI del ejercicio. La idea didáctica es que cada panel
// muestre una capa distinta del sistema: catálogo (Postgres + caché), checkout
// (reserva + idempotencia), prueba de carga (concurrencia), órdenes en vivo
// (estados) y ventas por minuto (Kafka + ClickHouse).

type Product = {
  id: number
  name: string
  description: string
  price_cents: number
  stock: number
  emoji: string
}

type Order = {
  id: string
  idempotency_key: string
  user_id: string
  product_id: number
  product_name: string
  qty: number
  total_cents: number
  status: string
  created_at: string
}

type SalePoint = {
  minute: string
  orders: number
  revenue: number
}

// Attempt existe separado de Order porque pending y failed no son órdenes:
// nunca llegaron a Postgres. Si se mezclaran en la misma lista, el panel
// sugeriría que un pago rechazado generó una orden, que es justo lo que el
// ejercicio quiere demostrar que NO pasa.
type Attempt = {
  id: string
  productName: string
  userId: string
  qty: number
  status: 'pending' | 'failed'
  idempotencyKey: string
  createdAt: string
}

type SimResult = {
  paid: number
  outOfStock: number
  failed: number
  other: number
}

type Outcome = {
  result: 'paid' | 'failed' | 'out_of_stock' | 'other'
  message: string
  orderId?: string
}

type Notice = {
  kind: 'ok' | 'error' | 'info'
  text: string
}

const money = (cents: number) =>
  `$${(cents / 100).toLocaleString('es-AR', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`

// readBody tolera respuestas no JSON para que un 502 del backend no reviente
// toda la UI con una excepción de parseo.
async function readBody(response: Response): Promise<Record<string, unknown>> {
  try {
    return (await response.json()) as Record<string, unknown>
  } catch {
    return {}
  }
}

function StatusBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    paid: 'bg-emerald-500/15 text-emerald-300 ring-emerald-500/30',
    failed: 'bg-rose-500/15 text-rose-300 ring-rose-500/30',
    pending: 'bg-amber-500/15 text-amber-300 ring-amber-500/30',
    out_of_stock: 'bg-orange-500/15 text-orange-300 ring-orange-500/30',
  }
  const className = styles[status] ?? 'bg-slate-500/15 text-slate-300 ring-slate-500/30'
  return (
    <span className={`inline-flex rounded-full px-2 py-0.5 text-[11px] font-semibold ring-1 ${className}`}>
      {status}
    </span>
  )
}

function StatCard({ label, value, hint, accent }: { label: string; value: string; hint: string; accent: string }) {
  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-4">
      <p className="text-[11px] font-semibold uppercase tracking-wider text-slate-500">{label}</p>
      <p className={`mt-2 text-2xl font-bold tabular-nums ${accent}`}>{value}</p>
      <p className="mt-1 text-xs text-slate-500">{hint}</p>
    </div>
  )
}

export default function App() {
  const [products, setProducts] = useState<Product[]>([])
  const [selectedId, setSelectedId] = useState<number>(1)
  const [qty, setQty] = useState<number>(1)
  const [idemKey, setIdemKey] = useState(() => `abc-${Math.random().toString(36).slice(2, 10)}`)
  const [orders, setOrders] = useState<Order[]>([])
  const [attempts, setAttempts] = useState<Attempt[]>([])
  const [sales, setSales] = useState<SalePoint[]>([])
  const [notice, setNotice] = useState<Notice | null>(null)
  const [busy, setBusy] = useState(false)
  const [simRunning, setSimRunning] = useState(false)
  const [simProgress, setSimProgress] = useState(0)
  const [simResult, setSimResult] = useState<SimResult | null>(null)

  const fetchProducts = useCallback(async () => {
    try {
      const response = await fetch('/api/products')
      if (response.ok) setProducts((await response.json()) as Product[])
    } catch {
      // El panel se refresca solo; un fallo temporal no rompe la UI.
    }
  }, [])

  const fetchOrders = useCallback(async () => {
    try {
      const response = await fetch('/api/orders')
      if (response.ok) setOrders((await response.json()) as Order[])
    } catch {
      // Las órdenes se consultan cada segundo: un fallo pasajero se corrige
      // solo en el próximo polling, no hace falta mostrar un error.
    }
  }, [])

  const fetchSales = useCallback(async () => {
    try {
      const response = await fetch('/api/analytics/sales')
      if (response.ok) {
        const body = (await response.json()) as { data?: SalePoint[] }
        setSales(body.data ?? [])
      }
    } catch {
      // ClickHouse es asíncrono y se consulta cada 3 s: un error temporal no
      // borra el gráfico anterior ni interrumpe el checkout.
    }
  }, [])

  // Tres frecuencias distintas: el catálogo cambia poco, las órdenes son el
  // panel "en vivo" y el gráfico depende de un pipeline asíncrono (Kafka).
  useEffect(() => {
    void fetchProducts()
    void fetchOrders()
    void fetchSales()
    const productTimer = window.setInterval(() => void fetchProducts(), 3000)
    const orderTimer = window.setInterval(() => void fetchOrders(), 1000)
    const salesTimer = window.setInterval(() => void fetchSales(), 3000)
    return () => {
      window.clearInterval(productTimer)
      window.clearInterval(orderTimer)
      window.clearInterval(salesTimer)
    }
  }, [fetchProducts, fetchOrders, fetchSales])

  const selected = products.find((product) => product.id === selectedId)
  const paidOrders = orders.filter((order) => order.status === 'paid')
  const revenue = paidOrders.reduce((sum, order) => sum + order.total_cents, 0)

  const chartData = useMemo(
    () =>
      sales.map((point) => ({
        label: point.minute.slice(11, 16),
        orders: Number(point.orders),
        revenue: Number(point.revenue),
      })),
    [sales],
  )

  // El panel mezcla intentos locales con órdenes confirmadas. Los intentos
  // muestran pending/failed antes de que Postgres tenga algo que contar; las
  // órdenes traen el total real. Se ordenan por fecha para que lo último arriba.
  const liveRows = useMemo(() => {
    const fromAttempts = attempts.map((attempt) => ({
      id: attempt.id,
      key: attempt.idempotencyKey,
      user: attempt.userId,
      product: attempt.productName,
      qty: attempt.qty,
      total: 0,
      status: attempt.status,
      time: attempt.createdAt,
    }))
    const fromOrders = orders.map((order) => ({
      id: order.id,
      key: order.idempotency_key,
      user: order.user_id,
      product: order.product_name,
      qty: order.qty,
      total: order.total_cents,
      status: order.status,
      time: order.created_at,
    }))
    return [...fromAttempts, ...fromOrders]
      .sort((a, b) => b.time.localeCompare(a.time))
      .slice(0, 30)
  }, [attempts, orders])

  const outcomeFrom = (body: Record<string, unknown>): Outcome => {
    if (body.status === 'paid') {
      const order = body.order as Order | undefined
      return {
        result: 'paid',
        orderId: order?.id,
        message: body.idempotent
          ? `Replay idempotente: ${order?.id?.slice(0, 8)} · no se cobró dos veces.`
          : `Pago aprobado · orden ${order?.id?.slice(0, 8)}`,
      }
    }
    if (body.status === 'payment_failed') {
      return { result: 'failed', message: 'Pago rechazado por el mock (5% de fallos aleatorios).' }
    }
    if (body.status === 'out_of_stock') {
      return { result: 'out_of_stock', message: 'Sin stock' }
    }
    return {
      result: 'other',
      message: typeof body.message === 'string' ? body.message : 'Respuesta inesperada',
    }
  }

  // La usa tanto el pago manual como la prueba de carga. Aceptar
  // already_reserved es clave para el doble click: dos reservas del mismo
  // usuario convergen en la misma clave de carrito y no descuentan dos veces.
  const reserveAndCheckout = async (
    userId: string,
    key: string,
    productId: number,
    quantity: number,
    onReserved?: () => void,
  ): Promise<Outcome> => {
    const reserveResponse = await fetch('/api/cart/reserve', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ user_id: userId, product_id: productId, qty: quantity }),
    })
    const reserve = await readBody(reserveResponse)
    if (reserve.status !== 'reserved' && reserve.status !== 'already_reserved') {
      return {
        result: 'out_of_stock',
        message: reserve.status === 'out_of_stock' ? 'Sin stock disponible' : 'No se pudo reservar el producto',
      }
    }

    onReserved?.()

    const checkoutResponse = await fetch('/api/checkout', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
      body: JSON.stringify({ user_id: userId, product_id: productId, qty: quantity }),
    })
    return outcomeFrom(await readBody(checkoutResponse))
  }

  // pending se inserta apenas se reservó, antes de los 500 ms del pago mock.
  // Si el pago falla se reemplaza por failed. Así el panel muestra los tres
  // estados aunque failed no llega nunca a la tabla orders de Postgres.
  const runCheckout = async (
    userId: string,
    key: string,
    productId: number,
    quantity: number,
  ): Promise<Outcome> => {
    const attemptId = `${key}-${Math.random().toString(36).slice(2, 7)}`
    const pendingAttempt: Attempt = {
      id: attemptId,
      productName: products.find((product) => product.id === productId)?.name ?? `Producto ${productId}`,
      userId,
      qty: quantity,
      status: 'pending',
      idempotencyKey: key,
      createdAt: new Date().toISOString(),
    }

    const outcome = await reserveAndCheckout(userId, key, productId, quantity, () => {
      setAttempts((previous) => [pendingAttempt, ...previous])
    })
    setAttempts((previous) => previous.filter((attempt) => attempt.id !== attemptId))

    if (outcome.result === 'failed') {
      setAttempts((previous) => [
        { ...pendingAttempt, id: `${attemptId}-failed`, status: 'failed', createdAt: new Date().toISOString() },
        ...previous,
      ])
    }
    return outcome
  }

  const refreshAll = async () => {
    await Promise.all([fetchProducts(), fetchOrders(), fetchSales()])
  }

  const handlePay = async () => {
    setBusy(true)
    setNotice(null)
    try {
      const outcome = await runCheckout(`user-${idemKey}`, idemKey, selectedId, qty)
      const kind = outcome.result === 'paid' ? 'ok' : outcome.result === 'other' ? 'info' : 'error'
      setNotice({ kind, text: outcome.message })
    } catch (error) {
      setNotice({ kind: 'error', text: String(error) })
    } finally {
      setBusy(false)
      await refreshAll()
    }
  }

  // Prueba del middleware de idempotencia: dos requests concurrentes con la
  // MISMA clave. Si el conteo da 2, el mutex + UNIQUE no están funcionando.
  const handleDoubleClick = async () => {
    setBusy(true)
    setNotice(null)
    const userId = `user-${idemKey}`
    try {
      const results = await Promise.all([
        runCheckout(userId, idemKey, selectedId, qty),
        runCheckout(userId, idemKey, selectedId, qty),
      ])
      const ids = new Set(results.map((item) => item.orderId).filter(Boolean))
      setNotice({
        kind: ids.size <= 1 ? 'ok' : 'error',
        text: `2 requests concurrentes · ${ids.size} orden(es) cobrada(s) · Idempotency-Key ${idemKey}.`,
      })
    } catch (error) {
      setNotice({ kind: 'error', text: String(error) })
    } finally {
      setBusy(false)
      await refreshAll()
    }
  }

  // 100 usuarios en paralelo, cada uno con su propia clave. El reparto final
  // no es fijo: el mock rechaza cerca del 5 % y esas reservas se liberan, así
  // que "compraron + sin stock" no tiene por qué ser exactamente 10 + 90.
  const handleSimulate = async () => {
    setSimRunning(true)
    setSimResult(null)
    setSimProgress(0)
    const result: SimResult = { paid: 0, outOfStock: 0, failed: 0, other: 0 }
    const stamp = Date.now()
    let done = 0

    const oneUser = async (index: number) => {
      const userId = `bf-user-${stamp}-${index}`
      const key = `bf-${stamp}-${index}`
      try {
        const outcome = await reserveAndCheckout(userId, key, 1, 1)
        if (outcome.result === 'paid') result.paid += 1
        else if (outcome.result === 'failed') result.failed += 1
        else if (outcome.result === 'out_of_stock') result.outOfStock += 1
        else result.other += 1
      } catch {
        result.other += 1
      }
      done += 1
      setSimProgress(done)
    }

    await Promise.all(Array.from({ length: 100 }, (_, index) => oneUser(index)))
    setSimResult(result)
    setSimRunning(false)
    await refreshAll()
  }

  const handleReset = async () => {
    await fetch('/api/dev/reset', { method: 'POST' })
    setSimResult(null)
    setSimProgress(0)
    await refreshAll()
  }

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      <div className="mx-auto max-w-7xl px-5 py-8">
        <header className="mb-8 flex flex-wrap items-end justify-between gap-4">
          <div>
            <p className="text-[11px] font-bold uppercase tracking-[0.22em] text-emerald-400">
              Ejercicio 0003 · Sistemas distribuidos
            </p>
            <h1 className="mt-2 text-3xl font-bold tracking-tight sm:text-4xl">¿Cómo funciona Shopify?</h1>
            <p className="mt-2 max-w-2xl text-sm leading-relaxed text-slate-400">
              Checkout que aguanta Black Friday: reservas en Redis, idempotencia en Postgres y ventas en vivo en
              ClickHouse.
            </p>
          </div>
          <div className="flex flex-wrap gap-2 text-[11px] font-semibold">
            {['Go', 'Postgres', 'Redis', 'Kafka', 'ClickHouse'].map((tag) => (
              <span key={tag} className="rounded-full border border-slate-700 px-3 py-1 text-slate-300">
                {tag}
              </span>
            ))}
          </div>
        </header>

        <section className="mb-6 grid grid-cols-2 gap-3 lg:grid-cols-4">
          <StatCard label="Productos" value={String(products.length)} hint="Catálogo desde Postgres" accent="text-slate-100" />
          <StatCard
            label={`Stock · ${selected?.name ?? '—'}`}
            value={String(selected?.stock ?? '—')}
            hint="Fuente de verdad: Postgres"
            accent={(selected?.stock ?? 0) > 0 ? 'text-emerald-400' : 'text-rose-400'}
          />
          <StatCard label="Órdenes pagadas" value={String(paidOrders.length)} hint="Últimas 100 en el API" accent="text-sky-400" />
          <StatCard label="Facturación" value={money(revenue)} hint="Ventas confirmadas" accent="text-emerald-400" />
        </section>

        <section className="mb-6 grid gap-6 lg:grid-cols-3">
          <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5 lg:col-span-2">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h2 className="text-lg font-semibold">Catálogo</h2>
                <p className="text-xs text-slate-500">Stock real, cacheado 30 s en Redis.</p>
              </div>
              <span className="rounded-md bg-slate-800 px-2 py-1 text-[11px] text-slate-400">
                GET /api/products
              </span>
            </div>
            <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-5">
              {products.map((product) => (
                <button
                  key={product.id}
                  type="button"
                  onClick={() => setSelectedId(product.id)}
                  className={`rounded-xl border p-3 text-left transition ${
                    product.id === selectedId
                      ? 'border-emerald-500 bg-emerald-500/10'
                      : 'border-slate-800 bg-slate-950/60 hover:border-slate-600'
                  }`}
                >
                  <div className="text-2xl">{product.emoji}</div>
                  <div className="mt-2 truncate text-xs font-semibold" title={product.name}>
                    {product.name}
                  </div>
                  <div className="mt-1 text-xs text-slate-400">{money(product.price_cents)}</div>
                  <div className={`mt-2 text-[11px] font-semibold ${product.stock > 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                    {product.stock > 0 ? `${product.stock} en stock` : 'Sin stock'}
                  </div>
                </button>
              ))}
            </div>
          </div>

          <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
            <h2 className="text-lg font-semibold">Checkout</h2>
            <p className="mt-1 text-xs text-slate-500">Reserva de 10 min + pago mock.</p>

            <label className="mt-4 block text-[11px] font-semibold uppercase tracking-wider text-slate-500">
              Idempotency-Key
            </label>
            <div className="mt-1 flex gap-2">
              <input
                value={idemKey}
                onChange={(event) => setIdemKey(event.target.value)}
                className="min-w-0 flex-1 rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 font-mono text-xs text-emerald-300 outline-none focus:border-emerald-500"
              />
              <button
                type="button"
                onClick={() => setIdemKey(`key-${Math.random().toString(36).slice(2, 10)}`)}
                className="rounded-lg border border-slate-700 px-2 text-xs text-slate-400 hover:bg-slate-800"
                title="Generar nueva clave"
              >
                ↻
              </button>
            </div>

            <div className="mt-4 grid grid-cols-2 gap-3">
              <div>
                <label className="block text-[11px] font-semibold uppercase tracking-wider text-slate-500">Cantidad</label>
                <input
                  type="number"
                  min={1}
                  value={qty}
                  onChange={(event) => setQty(Math.max(1, Number(event.target.value)))}
                  className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm outline-none focus:border-emerald-500"
                />
              </div>
              <div>
                <label className="block text-[11px] font-semibold uppercase tracking-wider text-slate-500">Total</label>
                <div className="mt-1 rounded-lg border border-slate-800 bg-slate-950 px-3 py-2 text-sm tabular-nums text-slate-300">
                  {money((selected?.price_cents ?? 0) * qty)}
                </div>
              </div>
            </div>

            <div className="mt-4 grid gap-2">
              <button
                type="button"
                disabled={busy || !selected}
                onClick={() => void handlePay()}
                className="rounded-lg bg-emerald-500 px-4 py-2.5 text-sm font-bold text-slate-950 transition hover:bg-emerald-400 disabled:opacity-50"
              >
                {busy ? 'Procesando…' : 'Reservar y pagar'}
              </button>
              <button
                type="button"
                disabled={busy || !selected}
                onClick={() => void handleDoubleClick()}
                className="rounded-lg border border-amber-500/60 bg-amber-500/10 px-4 py-2.5 text-sm font-semibold text-amber-300 transition hover:bg-amber-500/20 disabled:opacity-50"
              >
                Pagar (doble click)
              </button>
            </div>

            {notice && (
              <p
                className={`mt-4 rounded-lg border px-3 py-2 text-xs leading-relaxed ${
                  notice.kind === 'ok'
                    ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-300'
                    : notice.kind === 'error'
                      ? 'border-rose-500/30 bg-rose-500/10 text-rose-300'
                      : 'border-slate-700 bg-slate-800/60 text-slate-300'
                }`}
              >
                {notice.text}
              </p>
            )}
          </div>
        </section>

        <section className="mb-6 rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <h2 className="text-lg font-semibold">Prueba de carga · Black Friday</h2>
              <p className="mt-1 text-xs text-slate-500">
                100 checkouts en paralelo sobre el producto ID 1 (stock inicial 10). Cada usuario tiene clave propia.
              </p>
            </div>
            <div className="flex gap-2">
              <button
                type="button"
                onClick={() => void handleReset()}
                className="rounded-lg border border-slate-700 px-3 py-2 text-xs text-slate-300 hover:bg-slate-800"
              >
                Reset stock
              </button>
              <button
                type="button"
                disabled={simRunning}
                onClick={() => void handleSimulate()}
                className="rounded-lg bg-sky-500 px-4 py-2 text-sm font-bold text-slate-950 transition hover:bg-sky-400 disabled:opacity-50"
              >
                {simRunning ? `Simulando ${simProgress}/100…` : 'Simular 100 usuarios'}
              </button>
            </div>
          </div>

          {simRunning && (
            <div className="mt-4 h-1.5 overflow-hidden rounded-full bg-slate-800">
              <div
                className="h-full bg-sky-400 transition-all"
                style={{ width: `${simProgress}%` }}
              />
            </div>
          )}

          {simResult && (
            <div className="mt-5 grid grid-cols-2 gap-3 sm:grid-cols-4">
              <StatCard label="Compraron" value={String(simResult.paid)} hint="Orden pagada en Postgres" accent="text-emerald-400" />
              <StatCard label="Sin stock" value={String(simResult.outOfStock)} hint="Reserva o stock agotado" accent="text-orange-400" />
              <StatCard label="Pago fallido" value={String(simResult.failed)} hint="Mock 5% + payment_failed" accent="text-rose-400" />
              <StatCard
                label="Total"
                value={String(simResult.paid + simResult.outOfStock + simResult.failed + simResult.other)}
                hint="Debe sumar 100"
                accent="text-slate-100"
              />
            </div>
          )}
        </section>

        <section className="grid gap-6 lg:grid-cols-2">
          <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h2 className="text-lg font-semibold">Órdenes en vivo</h2>
                <p className="text-xs text-slate-500">Polling cada 1 s sobre Postgres.</p>
              </div>
              <span className="flex items-center gap-1.5 text-[11px] text-emerald-400">
                <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-emerald-400" /> live
              </span>
            </div>
            <div className="max-h-96 overflow-y-auto">
              <table className="w-full text-left text-xs">
                <thead className="sticky top-0 bg-slate-900 text-[10px] uppercase tracking-wider text-slate-500">
                  <tr>
                    <th className="pb-2 font-semibold">Orden</th>
                    <th className="pb-2 font-semibold">Producto</th>
                    <th className="pb-2 font-semibold">Total</th>
                    <th className="pb-2 font-semibold">Estado</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800">
                  {liveRows.length === 0 && (
                    <tr>
                      <td colSpan={4} className="py-8 text-center text-slate-600">
                        Todavía no hay órdenes.
                      </td>
                    </tr>
                  )}
                  {liveRows.map((row) => (
                    <tr key={row.id} className="text-slate-300">
                      <td className="py-2 font-mono text-[11px] text-slate-400">{row.id.slice(0, 8)}</td>
                      <td className="py-2">
                        <div className="truncate">{row.product}</div>
                        <div className="text-[10px] text-slate-600">
                          {row.user} · x{row.qty}
                        </div>
                      </td>
                      <td className="py-2 tabular-nums">{row.total > 0 ? money(row.total) : '—'}</td>
                      <td className="py-2">
                        <StatusBadge status={row.status} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h2 className="text-lg font-semibold">Ventas por minuto</h2>
                <p className="text-xs text-slate-500">order_created → Kafka → worker → ClickHouse.</p>
              </div>
              <span className="rounded-md bg-slate-800 px-2 py-1 text-[11px] text-slate-400">Recharts</span>
            </div>
            {chartData.length === 0 ? (
              <div className="flex h-64 flex-col items-center justify-center rounded-xl border border-dashed border-slate-800 text-center">
                <p className="text-sm text-slate-500">Sin datos todavía</p>
                <p className="mt-1 max-w-xs text-xs text-slate-600">
                  Confirmá una orden y el worker escribirá en ClickHouse en menos de un segundo.
                </p>
              </div>
            ) : (
              <ResponsiveContainer width="100%" height={264}>
                <ComposedChart data={chartData} margin={{ top: 8, right: 8, left: -12, bottom: 0 }}>
                  <CartesianGrid stroke="#1e293b" vertical={false} />
                  <XAxis dataKey="label" stroke="#64748b" tick={{ fontSize: 11 }} />
                  <YAxis
                    yAxisId="left"
                    stroke="#64748b"
                    tick={{ fontSize: 11 }}
                    tickFormatter={(value: number) => `$${value}`}
                  />
                  <YAxis
                    yAxisId="right"
                    orientation="right"
                    stroke="#64748b"
                    tick={{ fontSize: 11 }}
                    allowDecimals={false}
                  />
                  <Tooltip
                    contentStyle={{
                      background: '#0f172a',
                      border: '1px solid #334155',
                      borderRadius: 12,
                      color: '#e2e8f0',
                      fontSize: 12,
                    }}
                  />
                  <Legend wrapperStyle={{ fontSize: 11 }} />
                  <Area
                    yAxisId="left"
                    type="monotone"
                    dataKey="revenue"
                    name="Facturación"
                    stroke="#34d399"
                    fill="#10b981"
                    fillOpacity={0.25}
                    strokeWidth={2}
                  />
                  <Line
                    yAxisId="right"
                    type="monotone"
                    dataKey="orders"
                    name="Órdenes"
                    stroke="#60a5fa"
                    strokeWidth={2}
                    dot={false}
                  />
                </ComposedChart>
              </ResponsiveContainer>
            )}
          </div>
        </section>

        <footer className="mt-8 border-t border-slate-800 pt-5 text-xs text-slate-600">
          Reservas 10 min en Redis · descuento atómico en Postgres · eventos en Kafka · analítica en ClickHouse.
        </footer>
      </div>
    </div>
  )
}
