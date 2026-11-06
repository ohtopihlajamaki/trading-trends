// Showcase step 06: a headless Chromium opens the UI as a person would, at
// http://localhost:5173 in the UI container's network, and records what
// crosses edges 12 and 13:
//
//   OUT/requests.txt   every request the page makes: the Vue app's modules from
//                      Vite, then the API calls the Vite proxy passes on
//   OUT/http/*.json    the bodies of /symbols, /history and /signals
//   OUT/ws.jsonl       every message on the app's WebSocket (/ws), both ways
//   OUT/history-only.png  the page with the history loaded, before the live records
//   OUT/ui.png, OUT/price-chart.png  the page after the live records
//
// Once the page is live (status "live" and a stats frame) it writes OUT/.ready
// so run.sh can produce the records; it leaves once WANT_BARS bars and
// WANT_SIGNALS signals have come over the WebSocket and the chart has updated.
import { chromium } from 'playwright'
import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs'

const OUT = process.env.OUT
const UI = process.env.UI_URL ?? 'http://localhost:5173/'
const WANT_BARS = Number(process.env.WANT_BARS ?? 1)
const WANT_SIGNALS = Number(process.env.WANT_SIGNALS ?? 0)
const log = (...a) => console.log(new Date().toISOString(), ...a)
mkdirSync(`${OUT}/http`, { recursive: true })

const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } })
const requests = []
let bars = 0
let signals = 0
let statsFrames = 0

page.on('console', (m) => m.type() === 'error' && log(`console error: ${m.text()}`))
page.on('requestfinished', async (req) => {
  const res = await req.response()
  const u = new URL(req.url())
  requests.push(`${req.method()} ${u.pathname}${u.search} -> ${res?.status()} ${req.resourceType()}`)
  const api = u.pathname.match(/^\/(symbols|history|signals)$/)
  if (api && res) {
    const body = await res.text()
    writeFileSync(`${OUT}/http/${api[1]}.json`, body)
    log(`GET ${u.pathname}${u.search} -> ${res.status()}, ${body.length} bytes -> http/${api[1]}.json`)
  }
})
page.on('websocket', (ws) => {
  if (new URL(ws.url()).pathname !== '/ws') {
    log(`WebSocket ${ws.url()} (Vite's own, for reloading changed files; not recorded)`)
    return
  }
  log(`WebSocket ${ws.url()}`)
  const record = (dir, data) =>
    appendFileSync(`${OUT}/ws.jsonl`, `{"dir":"${dir}","ms":${Date.now()},"data":${data}}\n`)
  ws.on('framesent', (f) => record('sent', f.payload))
  ws.on('framereceived', (f) => {
    record('received', f.payload)
    const frame = JSON.parse(f.payload)
    bars += frame.bars.length
    signals += frame.signals.length
    if (frame.stats) statsFrames++
    if (frame.bars.length || frame.signals.length) log(`frame: ${frame.bars.length} bars, ${frame.signals.length} signals`)
  })
})

const until = async (what, ok, ms = 60000) => {
  const end = Date.now() + ms
  while (!ok()) {
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`)
    await page.waitForTimeout(100)
  }
}

await page.goto(UI)
await page.locator('.status.live').waitFor({ timeout: 60000 })
await until('the history', () => requests.some((r) => r.startsWith('GET /history')))
const seen = statsFrames
await until('a stats frame', () => statsFrames > seen)
await page.waitForTimeout(1000) // the chart has drawn the history
await page.screenshot({ path: `${OUT}/history-only.png` })
writeFileSync(`${OUT}/.ready`, '')
log('page live, history drawn')

await until('the live bars and signals', () => bars >= WANT_BARS && signals >= WANT_SIGNALS)
const after = statsFrames
await until('the next stats frame', () => statsFrames > after)
await page.waitForTimeout(500)
await page.screenshot({ path: `${OUT}/ui.png` })
await page.locator('section.price').screenshot({ path: `${OUT}/price-chart.png` })
writeFileSync(`${OUT}/requests.txt`, requests.join('\n') + '\n')
log(`${requests.length} requests, ${bars} bars and ${signals} signals over the WebSocket; screenshots saved`)
await browser.close()
