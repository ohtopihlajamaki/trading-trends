# 06 · gateway → Vite → browser

This step covers the last two edges of the graph:

| Edge | Between | What travels |
| --- | --- | --- |
| 12 | gateway ⇄ Vite dev server (`ui/vite.config.js`) | `/ws` (upgraded to a WebSocket), `/history`, `/signals`, `/symbols`, proxied unchanged to `GATEWAY_URL` |
| 13 | Vite ⇄ browser, port 5173 | the Vue app's files, then the app's HTTP calls and its WebSocket |

It runs the gateway as [step 05](../05-gateway/) does, the compose `ui` service in front of it, and a headless Chromium ([Playwright](https://playwright.dev)) that opens the UI. Then step 03's records are produced again, so the page receives them live.

## Run

```sh
./run.sh        # Docker, a .env and network access (npm) are enough
```

Run steps [01](../01-raw-to-prep/)–[04](../04-topics-to-clickhouse/) first; [05](../05-gateway/) too, if you want its answers compared with what the browser gets. The script:

1. Starts Redpanda and ClickHouse if they aren't running, and stops them again at the end.
2. Starts the gateway on fresh `showcase-live-*` topics and database `showcase`, as step 05 does.
3. **The UI.** Starts the compose `ui` service with `GATEWAY_URL=http://showcase-gateway:8000` and nothing else changed: `npm ci`, then `npm run dev` (Vite). It refuses to start if the `ui` service is already running, because both use the same `node_modules` volume.
4. **Edge 12.** Fetches the three API paths through Vite and straight from the gateway, and compares the bodies.
5. **Edge 13.**
   - Starts the browser in the UI container's network, so the page is `http://localhost:5173`, as on a laptop.
   - The image is `mcr.microsoft.com/playwright:v1.63.0-noble`, or `PLAYWRIGHT_IMAGE` from `.env`. It has the browsers; the script installs the matching `playwright` npm package into the container's `/tmp`.
6. Once the page is live, it produces step 03's records again, waits for the browser to finish, and compares its API bodies with step 05's.

It rewrites `output/` and prints:

```text
/symbols                     via Vite 418401775 170  gateway 418401775 170  identical
/history?symbol=A1PHBB.ETR   via Vite 2964863806 556  gateway 2964863806 556  identical
/signals?limit=5000          via Vite 252724125 201  gateway 252724125 201  identical
2026-11-05T16:40:35.232Z WebSocket ws://localhost:5173/?token=DkQmiUFpsM_0 (Vite's own, for reloading changed files; not recorded)
2026-11-05T16:40:35.237Z WebSocket ws://localhost:5173/ws
2026-11-05T16:40:35.346Z GET /symbols -> 200, 170 bytes -> http/symbols.json
2026-11-05T16:40:35.409Z GET /signals?limit=5000 -> 200, 201 bytes -> http/signals.json
2026-11-05T16:40:35.437Z GET /history?symbol=A1PHBB.ETR -> 200, 556 bytes -> http/history.json
2026-11-05T16:40:35.439Z GET /history?symbol=A1PHBB.ETR -> 200, 556 bytes -> http/history.json
2026-11-05T16:40:36.799Z page live, history drawn
2026-11-05T16:40:37.510Z frame: 4 bars, 0 signals
2026-11-05T16:40:37.526Z GET /history?symbol=A1PHBB.ETR -> 200, 556 bytes -> http/history.json
2026-11-05T16:40:37.716Z frame: 0 bars, 1 signals
2026-11-05T16:40:39.398Z 38 requests, 4 bars and 1 signals over the WebSocket; screenshots saved
browser http/symbols.json and step 05: identical
browser http/history.json and step 05: identical
browser http/signals.json and step 05: identical
screenshots: showcase/06-ui-and-browser/output/ui.png, showcase/06-ui-and-browser/output/price-chart.png, showcase/06-ui-and-browser/output/history-only.png
```

The checksums are `cksum`'s: CRC, then length in bytes.

## Edge 12: gateway ⇄ Vite

**In:** the gateway's answers, as in step 05.

**Inside Vite.** `ui/vite.config.js` proxies four paths to `GATEWAY_URL`:
- `/history`, `/signals` and `/symbols` as plain HTTP.
- `/ws` as a WebSocket (`ws: true`).

Everything else (the app) Vite serves itself.

**Out.** The same bytes. `output/proxy-check.txt` compares each API answer fetched through Vite with the same request straight to the gateway: identical, all three. The WebSocket goes through the same proxy, so the browser's frames below are the gateway's frames.

## Edge 13: Vite ⇄ browser

**What the page loads** (`output/requests.txt`, 38 requests):

| Requests | What |
| ---: | --- |
| 1 | `GET /`, the page |
| 29 | scripts: Vue, the app's components, and their dependencies, as Vite serves them in development (one module per file) |
| 3 | Perspective's WebAssembly files, for the signals grid |
| 5 | API calls through the proxy: `/symbols`, `/signals?limit=5000`, and `/history?symbol=A1PHBB.ETR` three times |

Then two WebSockets:
- `/ws`, the app's own.
- `/?token=…`, Vite's own, for reloading changed files during development. It isn't part of the pipeline and isn't recorded.

**What the app does** ([`ui/src/App.vue`](../../ui/src/App.vue), [`ui/src/api.js`](../../ui/src/api.js)):
- **On load.** It fetches `/symbols`, selects the first symbol (A1PHBB.ETR, the only one), and fetches its `/history` to draw the chart. It also fetches `/signals?limit=5000` for the grid.
- **The WebSocket.** It opens `/ws`, sends the subscription, and from then on applies each frame: bars of the selected symbol to the chart, every signal to the grid and the chart, stats to the header and the throughput chart.

The messages on `/ws` (`output/ws.jsonl`, the bars shortened):

```text
{"dir":"sent","ms":1793896835257,"data":{"subscribe":[]}}
{"dir":"sent","ms":1793896835342,"data":{"subscribe":["A1PHBB.ETR"]}}
{"dir":"sent","ms":1793896835344,"data":{"subscribe":["A1PHBB.ETR"]}}
{"dir":"received","ms":1793896835694,"data":{"bars":[],"signals":[],"dropped":0,"stats":{"bars_per_s":0,"e2e_latency_ms":0,"instance":"99331d501ea9","latency_ms":0,"signals_per_s":0,"t_ms":1793896835689,"window_ms":0}}}
{"dir":"received","ms":1793896836700,"data":{"bars":[],"signals":[],"dropped":0,"stats":{…}}}
{"dir":"received","ms":1793896837509,"data":{"bars":[…4 bars…],"signals":[],"dropped":0}}
{"dir":"received","ms":1793896837715,"data":{"bars":[],"signals":[{"symbol":"A1PHBB.ETR","window_start_ms":1636368000000,"side":"SELL","close":20.6,"ema38":20.988718623038146,"ema100":20.995884309536724,"trigger_sent_ms":1793893319539,"emitted_ms":1793896393647}],"dropped":0,"stats":{"bars_per_s":4,"e2e_latency_ms":3518099,"instance":"99331d501ea9","latency_ms":443991,"signals_per_s":1,"t_ms":1793896837689,"window_ms":1636368000000}}}
{"dir":"received","ms":1793896838700,"data":{"bars":[],"signals":[],"dropped":0,"stats":{…}}}
```

**Two things the UI does twice.** Neither does any harm, but both show in the recording:
- **On a first visit, it subscribes and loads the history twice.** With no symbol saved in `localStorage`, `loadSymbols()` selects the first symbol, and the `watch` on the selection subscribes and loads its history. Then `onMounted` does the same again. Hence `{"subscribe":[]}` on connect (nothing selected yet), two identical subscriptions, and two `/history` calls within 2 ms.
- **It reloads the history when old bars arrive.** The live bars are the same windows the chart already shows. `PriceChart.bar()` takes a bar older than the chart's last one for the start of a new replay, so the app reloads the history: the third `/history`. In the pipeline, live bars are newer than the history, and this happens only when a replay starts over.

**The page** (`output/ui.png`; `price-chart.png` is the chart alone, `history-only.png` the page before the live records):
- **Header:** status "live" and Replay clock "Mon 08 Nov, 11:40", which is the stats' `window_ms`.
  - E2E latency is 3,518,099 ms, measured from step 02's `sent_ms`, because these records were made in earlier steps.
  - Bars/s, Signals/s and Signal latency show the latest second, which was quiet again.
- **Symbols:** A1PHBB.ETR, Equity, 7 trades, last close 20.6, 1 signal. That's `/symbols`, with the type from step 04's `instruments`.
- **The chart:** four 5-minute candles (09:00, 09:05, 10:35, 11:40), EMA 38 and EMA 100 starting together at 21, EMA 38 dropping below EMA 100 at 11:40, and the SELL marker there.
- **Signals grid:** the SELL: 08/11/2021 11:40, close 20.60, EMA38 20.988719, EMA100 20.995884.
- **Throughput:** the second in which the 4 bars and the signal arrived.

**Changes on every run:** times, the gateway's `instance`, the latencies and Vite's token. The data repeats exactly.

## Snippet against the full run

**The same.** This is the pipeline's UI, unchanged, behind the pipeline's gateway. Only the gateway's topics and database are the demo ones.

**Different:**
- **Volume.** With a full replay running, the symbol list holds every symbol with bars, the grid fills with every symbol's signals, and the header shows the live replay's latencies (milliseconds) and rates.
- **Ports.** Nothing is published to the host here. To look at the page yourself, run the pipeline as `compose.yaml` describes and open http://localhost:5173.
