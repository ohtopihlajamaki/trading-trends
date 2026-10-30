# Real-time performance

Ohto-MacBook-Air, macOS 15.6.1, Apple M2, 8 cores; Docker with 8 CPUs and 7.7 GiB. Commit e17fc96, compose.yaml, gateway flush 250 ms, 1 signal instance(s).

| | p10 | p35 | p50 | p99 | p100 (busiest) |
|---|---|---|---|---|---|
| Window (Berlin time) | Mon 08.11. 21:10 | Thu 11.11. 07:15 | Tue 09.11. 13:20 | Fri 12.11. 09:05 | Wed 10.11. 17:35 |
| Ticks in the 5 min | 3136 | 24600 | 99696 | 116699 | 130515 |
| Throughput, ticks/s (avg / busiest second) | 10 / 159 | 82 / 1230 | 332 / 2584 | 389 / 2919 | 435 / 4247 |
| Window closes received / expected (bars) | 167 / 167 | 1230 / 1230 | 3011 / 3011 | 3174 / 3174 | 4236 / 4236 |
| BUY/SELL signals | 3 | 0 | 16 | 51 | 66 |
| Latency window end → shown, ms (median / slowest) | 144.0 / 644.0 | 331.0 / 331.0 | 79.0 / 79.0 | 93.0 / 93.0 | 58.0 / 58.0 |
| Latency window end → BUY/SELL shown, ms (median / slowest) | 144.0 / 144.0 | NA / NA | 79.0 / 79.0 | 32.0 / 93.0 | 58.0 / 58.0 |
| System latency, closing tick sent → shown, ms (median / slowest) | 140.0 / 154.0 | 219.0 / 222.0 | 92.0 / 93.0 | 93.0 / 93.0 | 56.0 / 56.0 |
| Median ms: wait for closing tick / signal service / gateway + flush | 4.0 / 10.0 / 130.0 | 112.0 / 10.0 / 202.0 | -13.0 / 12.0 / 80.0 | 0.0 / 10.0 / 82.0 | 2.0 / 16.0 / 40.0 |

Latency is measured by a WebSocket client (bench/probe) that subscribes to every symbol, like the UI does for one; median and slowest are over the bars of the window (one per symbol) or its signals. A window closes in one burst per partition, so its bars arrive in one or a few gateway frames: each column is essentially one closing event, and where it falls in the flush interval of the gateway (gateway + flush) moves it by up to that interval. A window closes only when its partition receives a record from after the window end, so the wait for the closing tick depends on how busy the market is, not on the system. For latency distributions over many closes, see the load sweep.
