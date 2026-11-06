// Showcase step 05: a client that does what the UI does, without a browser.
// Node's built-in fetch and WebSocket only, so the stock node image runs it.
//
// 1. Fetches /symbols, /history and /signals, as the UI does on load, and
//    saves each body in OUT/http/.
// 2. Opens /ws, subscribes to SYMBOL and saves every message, both ways, in
//    OUT/ws.jsonl. Once the first stats frame arrives it writes OUT/.ready,
//    so run.sh knows the subscription is live and can produce the records.
// 3. Leaves after WANT_BARS bars and WANT_SIGNALS signals, plus one stats frame.
import { mkdirSync, appendFileSync, writeFileSync } from 'node:fs'

const GATEWAY = process.env.GATEWAY ?? 'http://showcase-gateway:8000'
const OUT = process.env.OUT
const SYMBOL = process.env.SYMBOL ?? 'A1PHBB.ETR'
const WANT_BARS = Number(process.env.WANT_BARS ?? 1)
const WANT_SIGNALS = Number(process.env.WANT_SIGNALS ?? 0)
const log = (...a) => console.log(new Date().toISOString(), ...a)

mkdirSync(`${OUT}/http`, { recursive: true })

// --- 1. History over HTTP, the requests the UI makes on load ----------------
const requests = [
  ['symbols', '/symbols'],
  ['history', `/history?symbol=${encodeURIComponent(SYMBOL)}`],
  ['signals', '/signals?limit=5000'],
]
const summary = []
for (const [name, path] of requests) {
  const res = await fetch(GATEWAY + path)
  const body = await res.text()
  writeFileSync(`${OUT}/http/${name}.json`, body)
  summary.push(`GET ${path} -> ${res.status} ${res.headers.get('content-type')}, ${body.length} bytes -> http/${name}.json`)
  log(summary.at(-1))
  if (!res.ok) process.exit(1)
}
writeFileSync(`${OUT}/http/requests.txt`, summary.join('\n') + '\n')

// --- 2. The live feed over the WebSocket -------------------------------------
const record = (dir, data) =>
  appendFileSync(`${OUT}/ws.jsonl`, `{"dir":"${dir}","ms":${Date.now()},"data":${data}}\n`)

let bars = 0
let signals = 0
let ready = false
let done = false
const ws = new WebSocket(`${GATEWAY.replace(/^http/, 'ws')}/ws`)
const timeout = setTimeout(() => {
  log(`timed out with ${bars} bars and ${signals} signals`)
  process.exit(1)
}, 60000)

ws.onopen = () => {
  const msg = JSON.stringify({ subscribe: [SYMBOL] })
  ws.send(msg)
  record('sent', msg)
  log(`connected, sent ${msg}`)
}
ws.onmessage = (e) => {
  record('received', e.data)
  const f = JSON.parse(e.data)
  bars += f.bars.length
  signals += f.signals.length
  if (f.bars.length || f.signals.length) log(`frame: ${f.bars.length} bars, ${f.signals.length} signals`)
  if (f.stats && !ready) {
    ready = true
    writeFileSync(`${OUT}/.ready`, '')
    log('first stats frame: subscribed and live')
  }
  if (f.stats && done) {
    clearTimeout(timeout)
    ws.close()
  }
  if (bars >= WANT_BARS && signals >= WANT_SIGNALS && !done) {
    done = true
    log(`have ${bars} bars and ${signals} signals; waiting for the next stats frame`)
  }
}
ws.onclose = () => {
  log('closed')
  process.exit(done ? 0 : 1)
}
