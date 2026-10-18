<script setup>
// Bars and signals per second as the gateway sees them, last five minutes.
import { onBeforeUnmount, onMounted, ref } from 'vue'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'

const WINDOW = 300 // seconds kept
const el = ref(null)
let plot, resize, scheme
const data = [[], [], []]

const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim()

function build() {
  plot?.destroy()
  const axis = { stroke: css('--muted'), grid: { stroke: css('--grid') }, ticks: { stroke: css('--grid') } }
  plot = new uPlot({
    width: el.value.clientWidth,
    height: Math.max(el.value.clientHeight - 32, 50),
    cursor: { y: false },
    legend: { live: true },
    scales: { x: { time: true }, bars: { auto: true }, signals: { auto: true } },
    series: [
      {},
      { label: 'bars/s', scale: 'bars', stroke: css('--accent'), width: 2, value: (u, v) => v?.toLocaleString() ?? '–' },
      { label: 'signals/s', scale: 'signals', stroke: css('--ema38'), width: 2, value: (u, v) => v?.toLocaleString() ?? '–' },
    ],
    axes: [
      { ...axis },
      { ...axis, scale: 'bars', size: 60 },
      { ...axis, scale: 'signals', side: 1, size: 50, grid: { show: false } },
    ],
  }, data, el.value)
}

onMounted(() => {
  build()
  resize = new ResizeObserver(() => plot.setSize({ width: el.value.clientWidth, height: Math.max(el.value.clientHeight - 32, 50) }))
  resize.observe(el.value)
  scheme = matchMedia('(prefers-color-scheme: dark)')
  scheme.addEventListener('change', build)
})

onBeforeUnmount(() => {
  resize?.disconnect()
  scheme?.removeEventListener('change', build)
  plot?.destroy()
})

function add(stats) {
  data[0].push(stats.t_ms / 1000)
  data[1].push(stats.bars_per_s)
  data[2].push(stats.signals_per_s)
  while (data[0].length > WINDOW) data.forEach((d) => d.shift())
  plot?.setData(data)
}

defineExpose({ add })
</script>

<template>
  <div ref="el" class="plot"></div>
</template>

<style scoped>
.plot { flex: 1; min-height: 0; padding: 4px 8px 0; }
.plot :deep(.u-legend) { font-size: 12px; color: var(--muted); }
</style>
