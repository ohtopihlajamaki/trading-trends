<script setup>
// Every signal, newest first, in a Perspective datagrid that streams updates.
import { onBeforeUnmount, onMounted, ref } from 'vue'
import perspective from '@finos/perspective'
import perspective_viewer from '@finos/perspective-viewer'
import '@finos/perspective-viewer-datagrid'
import '@finos/perspective-viewer/dist/css/themes.css'
import SERVER_WASM from '@finos/perspective/dist/wasm/perspective-server.wasm?url'
import CLIENT_WASM from '@finos/perspective/dist/wasm/perspective-js.wasm?url'
import VIEWER_WASM from '@finos/perspective-viewer/dist/wasm/perspective-viewer.wasm?url'
import { formatFull } from '../time.js'

const emit = defineEmits(['select'])
const viewer = ref(null)
let table, scheme
let pending = []

// The worker and its WebAssembly load once per page
let worker
function client() {
  worker ??= Promise.all([
    perspective.init_server(fetch(SERVER_WASM)),
    perspective.init_client(fetch(CLIENT_WASM)),
    perspective_viewer.init_client(fetch(VIEWER_WASM)),
  ]).then(() => perspective.worker())
  return worker
}

const SCHEMA = {
  key: 'string', time: 'string', symbol: 'string', side: 'string', close: 'float', ema38: 'float',
  ema100: 'float', window_start: 'datetime',
}

// Berlin time as text: Perspective would show datetimes in the browser's zone.
// key makes a replayed signal replace its earlier copy.
const row = (s) => ({
  key: `${s.symbol}|${s.window_start_ms}`, time: formatFull(s.window_start_ms), symbol: s.symbol,
  side: s.side, close: s.close, ema38: s.ema38, ema100: s.ema100, window_start: s.window_start_ms,
})

const theme = () => viewer.value?.restore({ theme: scheme.matches ? 'Pro Dark' : 'Pro Light' })

// Prices as traded: up to 6 decimals (every field is spelled out for the viewer)
const PRICE = {
  number_format: {
    minimumIntegerDigits: null, minimumFractionDigits: 2, maximumFractionDigits: 6,
    minimumSignificantDigits: null, maximumSignificantDigits: null, roundingPriority: null,
    roundingIncrement: null, roundingMode: null, trailingZeroDisplay: null, useGrouping: null, signDisplay: null,
  },
}

onMounted(async () => {
  scheme = matchMedia('(prefers-color-scheme: dark)')
  scheme.addEventListener('change', theme)
  const c = await client()
  table = await c.table(SCHEMA, { index: 'key' })
  await viewer.value.load(table)
  await viewer.value.restore({
    plugin: 'Datagrid',
    columns: ['time', 'symbol', 'side', 'close', 'ema38', 'ema100'],
    sort: [['window_start', 'desc']],
    columns_config: { close: PRICE, ema38: PRICE, ema100: PRICE },
    settings: false,
    theme: scheme.matches ? 'Pro Dark' : 'Pro Light',
  })
  if (pending.length) {
    await table.update(pending)
    pending = []
  }
  viewer.value.addEventListener('perspective-click', (e) => {
    const symbol = e.detail?.row?.symbol
    if (symbol) emit('select', symbol)
  })
})

onBeforeUnmount(async () => {
  scheme?.removeEventListener('change', theme)
  await viewer.value?.delete()
  await table?.delete()
})

// add appends signals (rows as the gateway sends them)
function add(signals) {
  if (!signals.length) return
  const rows = signals.map(row)
  if (table) table.update(rows)
  else pending.push(...rows)
}

defineExpose({ add })
</script>

<template>
  <perspective-viewer ref="viewer" class="grid"></perspective-viewer>
</template>

<style scoped>
.grid { flex: 1; min-height: 0; }
</style>
