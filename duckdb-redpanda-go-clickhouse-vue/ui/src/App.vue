<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import SymbolList from './components/SymbolList.vue'
import PriceChart from './components/PriceChart.vue'
import SignalsGrid from './components/SignalsGrid.vue'
import ThroughputChart from './components/ThroughputChart.vue'
import { Live, getJSON, rows } from './api.js'
import { formatDayHM } from './time.js'

const symbols = ref([])
const stored = (() => { try { return localStorage.getItem('symbol') ?? '' } catch { return '' } })()
const selected = ref(stored)
const status = ref('connecting')
const stats = ref(null)
const lastE2ELatency = ref(null)
const error = ref('')
const chart = ref(null)
const grid = ref(null)
const throughput = ref(null)
let live, refresh
let loading = null // symbol whose history is being fetched
// Live bars and signals of the selected symbol. ClickHouse lags the live feed
// by a few seconds, so after every history load these fill in what it lacks.
let recentBars = []
let recentSignals = []
const RECENT = 5000

const current = computed(() => symbols.value.find((s) => s.symbol === selected.value))

async function loadSymbols() {
  try {
    symbols.value = rows(await getJSON('/symbols'))
    if (!selected.value && symbols.value.length) select(symbols.value[0].symbol)
    error.value = ''
  } catch (e) {
    error.value = e.message
  }
}

async function loadHistory(symbol) {
  loading = symbol
  try {
    const h = await getJSON(`/history?symbol=${encodeURIComponent(symbol)}`)
    if (symbol !== selected.value) return
    chart.value.load(h.bars, h.signals)
    recentBars.forEach((b) => chart.value.bar(b))
    recentSignals.forEach((s) => chart.value.signal(s))
    chart.value.fit()
  } catch (e) {
    error.value = e.message
  } finally {
    if (loading === symbol) loading = null
  }
}

function select(symbol) {
  if (symbol) selected.value = symbol
}

watch(selected, (symbol) => {
  if (!symbol) return
  try { localStorage.setItem('symbol', symbol) } catch {}
  recentBars = []
  recentSignals = []
  live?.subscribe([symbol])
  loadHistory(symbol)
})

function onFrame(f) {
  for (const b of f.bars) {
    if (b.symbol !== selected.value) continue
    // A bar older than the last one means a new replay has started
    if (b.window_start_ms < (recentBars.at(-1)?.window_start_ms ?? 0)) {
      recentBars = []
      recentSignals = []
      lastE2ELatency.value = null
    }
    recentBars.push(b)
    if (recentBars.length > RECENT) recentBars.shift()
    if (loading !== b.symbol && !chart.value.bar(b)) loadHistory(b.symbol)
  }
  if (f.signals.length) {
    grid.value.add(f.signals)
    for (const s of f.signals) {
      if (s.symbol !== selected.value) continue
      recentSignals.push(s)
      if (recentSignals.length > RECENT) recentSignals.shift()
      if (loading !== s.symbol) chart.value.signal(s)
    }
  }
  if (f.stats) {
    stats.value = f.stats
    if (f.stats.e2e_latency_ms > 0) {
      lastE2ELatency.value = f.stats.e2e_latency_ms
    }
    throughput.value.add(f.stats)
  }
}

onMounted(async () => {
  live = new Live({ onFrame, onStatus: (s) => (status.value = s) })
  await loadSymbols()
  if (selected.value) {
    live.subscribe([selected.value])
    loadHistory(selected.value)
  }
  try {
    grid.value.add(rows(await getJSON('/signals?limit=5000')))
  } catch (e) {
    error.value = e.message
  }
  refresh = setInterval(loadSymbols, 30000)
})

onBeforeUnmount(() => {
  live?.close()
  clearInterval(refresh)
})

const fmt = (n) => (n == null ? '–' : n.toLocaleString())
</script>

<template>
  <div class="app">
    <header class="top">
      <h1>Stock Signals</h1>
      <span class="status" :class="status">{{ status }}</span>
      <dl class="kpis">
        <div><dt>Replay clock</dt><dd>{{ stats?.window_ms ? formatDayHM(stats.window_ms) : '–' }}</dd></div>
        <div><dt>E2E latency</dt><dd>{{ lastE2ELatency != null ? `${fmt(Math.round(lastE2ELatency))} ms` : '–' }}</dd></div>
        <div><dt>Bars/s</dt><dd>{{ fmt(stats?.bars_per_s) }}</dd></div>
        <div><dt>Signals/s</dt><dd>{{ fmt(stats?.signals_per_s) }}</dd></div>
        <div><dt>Signal latency</dt><dd>{{ stats ? `${fmt(Math.round(stats.latency_ms))} ms` : '–' }}</dd></div>
      </dl>
    </header>
    <p v-if="error" class="error">{{ error }}</p>

    <SymbolList class="side" :symbols="symbols" :selected="selected" @select="select" />

    <section class="panel price">
      <header>
        {{ selected || 'No symbol' }}
        <span v-if="current" class="sub">
          {{ current.exchange }} · {{ current.sectype === 'I' ? 'Index' : 'Equity' }} · {{ current.bars }} bars ·
          {{ current.signals }} signals
        </span>
        <span class="legend sub">
          <i class="swatch ema38"></i>EMA 38 <i class="swatch ema100"></i>EMA 100 · 5-minute bars, Europe/Berlin
        </span>
      </header>
      <PriceChart ref="chart" />
    </section>

    <section class="panel signals">
      <header>Signals <span class="sub">newest first · click a row to chart it</span></header>
      <SignalsGrid ref="grid" @select="select" />
    </section>

    <section class="panel rate">
      <header>Throughput <span class="sub">gateway {{ stats?.instance ?? '' }}</span></header>
      <ThroughputChart ref="throughput" />
    </section>

    <footer class="foot muted">
      Data: DEBS 2022 Grand Challenge Trading Data © Infront Financial Technology GmbH, licensed under
      <a href="http://creativecommons.org/licenses/by-nc-sa/4.0/" target="_blank" rel="noopener">CC BY-NC-SA 4.0</a>
      (<a href="https://doi.org/10.5281/zenodo.6382482" target="_blank" rel="noopener">source</a>).
    </footer>
  </div>
</template>

<style scoped>
.app {
  height: 100%;
  display: grid;
  gap: 8px;
  padding: 8px;
  grid-template-columns: 260px minmax(0, 2fr) minmax(0, 1fr);
  grid-template-rows: auto auto minmax(0, 3fr) minmax(0, 2fr) auto;
  grid-template-areas:
    "top top top"
    "err err err"
    "side price price"
    "side signals rate"
    "foot foot foot";
}
.top { grid-area: top; display: flex; align-items: center; gap: 16px; flex-wrap: wrap; padding: 0 4px; }
h1 { font-size: 18px; margin: 0; }
.status { font-size: 12px; padding: 2px 8px; border-radius: 999px; border: 1px solid var(--border); color: var(--muted); }
.status.live { color: var(--buy); border-color: currentColor; }
.status.reconnecting { color: var(--sell); border-color: currentColor; }
.kpis { display: flex; gap: 24px; margin: 0 0 0 auto; flex-wrap: wrap; }
.kpis div { display: flex; flex-direction: column; }
dt { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; }
dd { margin: 0; font-weight: 600; }
.error { grid-area: err; margin: 0; padding: 6px 10px; border-radius: 6px; color: var(--sell); border: 1px solid currentColor; }
.side { grid-area: side; }
.price { grid-area: price; }
.signals { grid-area: signals; }
.rate { grid-area: rate; }
.foot { grid-area: foot; font-size: 12px; padding: 0 4px; }
.foot a { color: inherit; }
.legend { margin-left: auto; }
.swatch { display: inline-block; width: 12px; height: 3px; vertical-align: middle; margin: 0 4px 0 8px; border-radius: 2px; }
.swatch.ema38 { background: var(--ema38); }
.swatch.ema100 { background: var(--ema100); }

@media (max-width: 900px) {
  .app {
    height: auto;
    grid-template-columns: minmax(0, 1fr);
    grid-template-rows: auto auto 320px 420px 360px 240px auto;
    grid-template-areas: "top" "err" "side" "price" "signals" "rate" "foot";
  }
  .legend { display: none; }
}
</style>
