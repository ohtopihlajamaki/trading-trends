# Real-time performance

khmbp, macOS 27.0.1, Apple M1 Pro, 8 cores; Docker with 8 CPUs and 7.7 GiB. Commit 23c1432-dirty, compose.yaml, gateway flush 250 ms, 1 signal instance(s).

| | p50 | p100 (busiest) |
|---|---|---|
| Window (Berlin time) | Mon 08.11. 14:20 | Mon 08.11. 17:35 |
| Ticks in the 5 min | 995590 | 1269790 |
| Throughput, ticks/s (avg / busiest second) | 3319 / 26270 | 4233 / 39260 |
| Window closes received / expected (bars) | 20013 / 30210 | 20000 / 42140 |
| BUY/SELL signals | 340 | 1000 |
| Latency window end → shown, ms (median / slowest) | 189.0 / 189.0 | 314.0 / 314.0 |
| Latency window end → BUY/SELL shown, ms (median / slowest) | 189.0 / 189.0 | 314.0 / 314.0 |
| System latency, closing tick sent → shown, ms (median / slowest) | 183.0 / 190.0 | 315.0 / 315.0 |
| Median ms: wait for closing tick / signal service / gateway + flush | 6.0 / 57.0 / 127.0 | -1.0 / 67.0 / 248.0 |

Latency is measured by a WebSocket client (bench/probe) that subscribes to every symbol, like the UI does for one; median and slowest are over the bars of the window (one per symbol) or its signals. A window closes in one burst per partition, so its bars arrive in one or a few gateway frames: each column is essentially one closing event, and where it falls in the flush interval of the gateway (gateway + flush) moves it by up to that interval. A window closes only when its partition receives a record from after the window end, so the wait for the closing tick depends on how busy the market is, not on the system. For latency distributions over many closes, see the load sweep.
