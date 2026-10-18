<script setup>
// Candles, EMA(38) and EMA(100) of one symbol, with BUY/SELL markers. The
// parent loads the history, then feeds live bars and signals.
import { onBeforeUnmount, onMounted, ref } from 'vue'
import { CandlestickSeries, LineSeries, createChart, createSeriesMarkers } from 'lightweight-charts'
import { formatDay, formatDayHM, formatHM } from '../time.js'

const el = ref(null)
let chart, candles, ema38, ema100, markers, scheme
let lastTime = 0
let marks = []

const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim()
const sec = (ms) => Math.floor(ms / 1000)

function theme() {
  chart.applyOptions({
    layout: { background: { color: css('--panel') }, textColor: css('--muted'), attributionLogo: false },
    grid: { vertLines: { color: css('--grid') }, horzLines: { color: css('--grid') } },
    rightPriceScale: { borderColor: css('--border') },
    timeScale: { borderColor: css('--border') },
  })
  candles.applyOptions({
    upColor: css('--buy'), downColor: css('--sell'), borderVisible: false,
    wickUpColor: css('--buy'), wickDownColor: css('--sell'),
  })
  ema38.applyOptions({ color: css('--ema38') })
  ema100.applyOptions({ color: css('--ema100') })
  markers.setMarkers(marks.map(styleMark))
}

function styleMark(m) {
  const buy = m.side === 'BUY'
  return {
    time: m.time, position: buy ? 'belowBar' : 'aboveBar', shape: buy ? 'arrowUp' : 'arrowDown',
    color: css(buy ? '--buy' : '--sell'), text: m.side,
  }
}

onMounted(() => {
  chart = createChart(el.value, {
    autoSize: true,
    localization: { timeFormatter: (t) => formatDayHM(t * 1000) },
    timeScale: {
      timeVisible: true,
      // Day boundaries show the date, everything else the Berlin time
      tickMarkFormatter: (t, type) => (type <= 2 ? formatDay(t * 1000) : formatHM(t * 1000)),
    },
    crosshair: { mode: 0 },
  })
  candles = chart.addSeries(CandlestickSeries, { priceLineVisible: false })
  const line = { lineWidth: 2, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false }
  ema38 = chart.addSeries(LineSeries, { ...line, title: 'EMA 38' })
  ema100 = chart.addSeries(LineSeries, { ...line, title: 'EMA 100' })
  markers = createSeriesMarkers(candles, [])
  theme()
  scheme = matchMedia('(prefers-color-scheme: dark)')
  scheme.addEventListener('change', theme)
})

onBeforeUnmount(() => {
  scheme?.removeEventListener('change', theme)
  chart?.remove()
})

// load replaces everything with a symbol's history (columnar, as /history returns it)
function load(bars, signals) {
  const t = bars.t ?? []
  candles.setData(t.map((ms, i) => ({
    time: sec(ms), open: bars.open[i], high: bars.high[i], low: bars.low[i], close: bars.close[i],
  })))
  ema38.setData(t.map((ms, i) => ({ time: sec(ms), value: bars.ema38[i] })))
  ema100.setData(t.map((ms, i) => ({ time: sec(ms), value: bars.ema100[i] })))
  marks = (signals.window_start_ms ?? []).map((ms, i) => ({ time: sec(ms), side: signals.side[i] }))
  markers.setMarkers(marks.map(styleMark))
  lastTime = t.length ? sec(t[t.length - 1]) : 0
}

const fit = () => chart.timeScale().fitContent()

// bar applies a live bar; it returns false when the bar is older than the
// chart (a new replay), so the caller reloads the history instead
function bar(b) {
  const time = sec(b.window_start_ms)
  if (time < lastTime) return false
  lastTime = time
  candles.update({ time, open: b.open, high: b.high, low: b.low, close: b.close })
  ema38.update({ time, value: b.ema38 })
  ema100.update({ time, value: b.ema100 })
  return true
}

function signal(s) {
  const time = sec(s.window_start_ms)
  if (marks.some((m) => m.time === time)) return // already in the history
  marks.push({ time, side: s.side })
  marks.sort((a, b) => a.time - b.time)
  markers.setMarkers(marks.map(styleMark))
}

defineExpose({ load, bar, signal, fit })
</script>

<template>
  <div ref="el" class="chart"></div>
</template>

<style scoped>
.chart { flex: 1; min-height: 0; }
</style>
