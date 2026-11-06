# Real-time performance

khmbp, macOS 27.0.1, Apple M1 Pro, 8 cores; Docker with 8 CPUs and 7.7 GiB. Commit 23c1432-dirty, compose.yaml, gateway flush 250 ms, 1 signal instance(s).

| | p50 | p100 (busiest) |
|---|---|---|
| Window (Berlin time) | Mon 08.11. 14:20 | Mon 08.11. 17:35 |
| Ticks in the 5 min | 1991180 | 2539580 |
| Throughput, ticks/s (avg / busiest second) | 6637 / 52540 | 8465 / 78520 |
| Window closes received / expected (bars) | 60420 / 60420 | 84280 / 84280 |
| BUY/SELL signals | 680 | 2000 |
| Latency window end → shown, ms (median / slowest) | 363.0 / 363.0 | 296.0 / 487.0 |
| Latency window end → BUY/SELL shown, ms (median / slowest) | 363.0 / 363.0 | 296.0 / 487.0 |
| System latency, closing tick sent → shown, ms (median / slowest) | 357.0 / 361.0 | 293.0 / 482.0 |
| Median ms: wait for closing tick / signal service / gateway + flush | 6.0 / 65.0 / 290.0 | 4.0 / 72.0 / 236.0 |

Latency is measured by a WebSocket client (bench/probe) that subscribes to every symbol, like the UI does for one; median and slowest are over the bars of the window (one per symbol) or its signals. A window closes in one burst per partition, so its bars arrive in one or a few gateway frames: each column is essentially one closing event, and where it falls in the flush interval of the gateway (gateway + flush) moves it by up to that interval. A window closes only when its partition receives a record from after the window end, so the wait for the closing tick depends on how busy the market is, not on the system. For latency distributions over many closes, see the load sweep.
