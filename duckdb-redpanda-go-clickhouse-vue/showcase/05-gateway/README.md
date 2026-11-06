# 05 · ClickHouse and topics → gateway → client

This step covers edges 10 and 11 of the graph, the gateway's two inputs:

| Edge | From | What the gateway does | To the client |
| --- | --- | --- | --- |
| 11 | ClickHouse, database `showcase` from [step 04](../04-topics-to-clickhouse/) | runs one query per request, `FORMAT JSONColumns` | `GET /symbols`, `/history?symbol=`, `/signals?limit=` |
| 10 | the `bars_ema` and `signals` topics | reads them as group `showcase-gateway`, from the live end, and forwards each value as it is | WebSocket frames on `/ws`: bars of the subscribed symbols, every signal, stats once a second |

The gateway is built from this checkout. A small client ([`client.mjs`](client.mjs), Node's built-in `fetch` and `WebSocket`, no packages) makes the requests the UI makes and saves what comes back. [Step 06](../06-ui-and-browser/) does the same with the real UI in a browser.

## Run

```sh
./run.sh        # Docker and a .env are enough
```

Run steps [01](../01-raw-to-prep/)–[04](../04-topics-to-clickhouse/) first. The script:

1. Starts Redpanda and ClickHouse if they aren't running, and stops them again at the end.
2. Checks that database `showcase` holds step 03's 4 bars and 1 signal.
3. Creates two fresh topics, `showcase-live-bars_ema` and `showcase-live-signals`.
4. Starts the gateway on those topics and on database `showcase`, and waits for `/healthz`.
5. Starts the client. When the client is subscribed, the script produces step 03's records again, with the same partition, key and value bytes.
6. Waits for the client to finish, saves the gateway's counters and log, and checks that every record reached the client unchanged.

It rewrites `output/` and prints:

```text
2026-11-05T16:37:05.966Z GET /symbols -> 200 application/json, 170 bytes -> http/symbols.json
2026-11-05T16:37:05.987Z GET /history?symbol=A1PHBB.ETR -> 200 application/json, 556 bytes -> http/history.json
2026-11-05T16:37:05.995Z GET /signals?limit=5000 -> 200 application/json, 201 bytes -> http/signals.json
2026-11-05T16:37:05.999Z connected, sent {"subscribe":["A1PHBB.ETR"]}
2026-11-05T16:37:06.610Z first stats frame: subscribed and live
2026-11-05T16:37:07.073Z frame: 4 bars, 0 signals
2026-11-05T16:37:07.248Z frame: 0 bars, 1 signals
2026-11-05T16:37:07.248Z have 4 bars and 1 signals; waiting for the next stats frame
2026-11-05T16:37:07.751Z closed
all 4 bars and 1 signals of step 03 reached the client byte for byte; 4 frames in showcase/05-gateway/output/ws.jsonl
```

**Why the records are produced again.** The gateway starts at the end of its topics and never commits. Records already in step 03's topics would never reach it, so the step needs records that arrive after the gateway has joined.
- **The bytes are the same.** The script sends step 03's `bars_ema.jsonl` and `signals.jsonl` again with `rpk topic produce`: same partition (27), same key, same value bytes. In the pipeline, the signal service produces them while the gateway runs.
- **Settings.** Only `BARS_TOPIC`, `SIGNALS_TOPIC`, `GATEWAY_GROUP` and `CLICKHOUSE_URL` (`http://clickhouse:8123/?database=showcase`) differ from the compose service. The gateway's queries keep the URL's `?database=`, so they read `showcase`.
- **Ports.** The gateway publishes no port, and is reached as `showcase-gateway:8000` on the compose network.

To remove the live topics:

```sh
docker compose exec redpanda rpk topic delete showcase-live-bars_ema showcase-live-signals
```

## Edge 11: ClickHouse → gateway → HTTP

**In:** database `showcase`, as step 04 left it. The gateway reads `bars_ema`, `signals` and `instruments`, never `ticks` or `bars_5m`.

**Inside the gateway** ([`gateway/main.go`](../../gateway/main.go)). Each endpoint is one or two SQL queries over ClickHouse's HTTP interface, with the request's values as `{name:Type}` parameters:
- **`/history`** reads `bars_ema FINAL` and `signals FINAL` for the symbol, and joins the two results into one object.
- **`/signals`** returns the most recent signals.
- **`/symbols`** returns one row per symbol with bars: counts, the last close, and the type and exchange from `instruments`.

`FINAL` makes ReplacingMergeTree drop duplicate rows at read time. ClickHouse answers in `JSONColumns` (one array per column), and the gateway passes that on unchanged.

**Out:** `output/http/`. `history.json`, which is what the UI draws the chart from:

```json
{"symbol":"A1PHBB.ETR","bars":{
	"t": [1636358400000, 1636358700000, 1636364100000, 1636368000000],
	"open": [21, 21.2, 21.2, 20.6],
	"high": [21, 21.2, 21.2, 20.6],
	"low": [21, 21.2, 21, 20.6],
	"close": [21, 21.2, 21, 20.6],
	"trades": [3, 1, 2, 1],
	"ema38": [21, 21.01025641025641, 21.00973044049967, 20.988718623038146],
	"ema100": [21, 21.003960396039606, 21.00388197235565, 20.995884309536724]
},"signals":{
	"window_start_ms": [1636368000000],
	"side": ["SELL"],
	"close": [20.6],
	"ema38": [20.988718623038146],
	"ema100": [20.995884309536724]
}}
```

`symbols.json` brings in step 04's `instruments` row, which supplies `sectype` and `exchange`:

```json
{
	"symbol": ["A1PHBB.ETR"],
	"sectype": ["E"],
	"exchange": ["ETR"],
	"bars": [4],
	"trades": [7],
	"last_close": [20.6],
	"last_ms": [1636368000000],
	"signals": [1]
}
```

`signals.json` is the same SELL with its `emitted_ms` (`1793896393647`, step 03's), read back from `emitted_at`.

What changes from the tables to the response:
- **Times.** `window_start` (a DateTime in Berlin) becomes epoch milliseconds again: `t`, `window_start_ms`, `last_ms`.
- **`side`** goes from an Enum to text.
- **Layout.** Rows become columns.
- **`trigger_sent_ms`** never reached ClickHouse (step 04 skips it), so history has no latencies. Only the live path carries them.

## Edge 10: topics → gateway → WebSocket

**In:** the 4 bars and the SELL, in partition 27 of `showcase-live-bars_ema` and `showcase-live-signals`.

**Inside the gateway** ([`gateway/hub.go`](../../gateway/hub.go)):
- **Reading.** It reads only `symbol`, `window_start_ms`, `trigger_sent_ms` and `emitted_ms` of each record, for routing and for its statistics.
- **Routing.** A bar goes to the clients subscribed to its symbol. A signal goes to every client.
- **Batching.** Each client gets one frame per flush (`FLUSH_MS`, 250 ms) holding everything pending, plus a stats summary once a second. A client that falls behind loses its oldest messages, counted in `dropped`.

**Out:** `output/ws.jsonl`: every message both ways, with the client's arrival time in `ms`. The four frames received, shortened:

```text
{"dir":"sent","ms":1793896625998,"data":{"subscribe":["A1PHBB.ETR"]}}
{"dir":"received","ms":1793896626608,"data":{"bars":[],"signals":[],"dropped":0,"stats":{"bars_per_s":0,"e2e_latency_ms":0,"instance":"e155d75a21d2","latency_ms":0,"signals_per_s":0,"t_ms":1793896626605,"window_ms":0}}}
{"dir":"received","ms":1793896627071,"data":{"bars":[{…09:00…},{…09:05…},{…10:35…},{"symbol":"A1PHBB.ETR","window_start_ms":1636368000000,"open":20.6,"high":20.6,"low":20.6,"close":20.6,"trades":1,"ema38":20.988718623038146,"ema100":20.995884309536724,"trigger_sent_ms":1793893319539,"emitted_ms":1793896393647}],"signals":[],"dropped":0}}
{"dir":"received","ms":1793896627247,"data":{"bars":[],"signals":[{"symbol":"A1PHBB.ETR","window_start_ms":1636368000000,"side":"SELL","close":20.6,"ema38":20.988718623038146,"ema100":20.995884309536724,"trigger_sent_ms":1793893319539,"emitted_ms":1793896393647}],"dropped":0}}
{"dir":"received","ms":1793896627747,"data":{"bars":[],"signals":[],"dropped":0,"stats":{"bars_per_s":4,"e2e_latency_ms":3307658,"instance":"e155d75a21d2","latency_ms":233550,"signals_per_s":1,"t_ms":1793896627605,"window_ms":1636368000000}}}
```

- **Values arrive unchanged.** Each bar and signal is the record's value, byte for byte. The script checks that every value in step 03's files appears in a frame. Unlike history, the live messages keep `trigger_sent_ms` and `emitted_ms`.
- **Separate frames.** The bars and the SELL came in two frames because the script produced the two topics one after the other. In the pipeline the signal service sends them in one batch.
- **Stats** reach every client once a second:
  - `bars_per_s` and `signals_per_s` count the second before.
  - `window_ms` is the latest window seen, which is what the UI shows as "Replay clock".
  - `latency_ms` is the largest receive time − `emitted_ms` among that second's signals, and `e2e_latency_ms` the largest receive time − `trigger_sent_ms`.

  Here those last two are about 3.9 minutes and 55 minutes, because the records were made in steps 03 and 02. In a live replay they are milliseconds.

The gateway's counters (`output/metrics.txt`):

```text
gateway_http_seconds_count{endpoint="history"} 1
gateway_http_seconds_count{endpoint="signals"} 1
gateway_http_seconds_count{endpoint="symbols"} 1
gateway_records_total{topic="showcase-live-bars_ema"} 4
gateway_records_total{topic="showcase-live-signals"} 1
gateway_ws_clients 0
gateway_ws_dropped_total 0
gateway_ws_frames_total 4
```

**Changes on every run:** the `ms`, `t_ms` and `instance` fields and the two latencies. Everything else repeats exactly.

## Snippet against the full run

**The same.** The queries and the frame format are what the pipeline's gateway uses with database `default` and topics `bars_ema` and `signals`.

**Different:**
- **History size.** In a full run, `/history` returns every window of the symbol that ClickHouse holds (a day or a week, depending on `DAYS`), and `/symbols` about 4,800 rows for Monday.
- **Frames.** Frames carry up to 250 ms of bars for the subscribed symbols. They also carry every signal of every symbol, which is why the UI's signal grid fills while one chart is shown.
