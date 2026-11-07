# Real-time performance (20x, not real time)

bench, Ubuntu 26.04.1 LTS (kernel 7.0.0-43-generic), AMD Ryzen 5 5600 6-Core Processor, 12 cores; Docker with 12 CPUs and 11.1 GiB. Commit 3eef762-dirty, compose.yaml:compose.perf.yaml, gateway flush 250 ms, 3 signal instance(s).

| | p10 | p35 | p50 | p99 | p100 (busiest) |
|---|---|---|---|---|---|
| Window (Berlin time) | Mon 08.11. 21:10 | Thu 11.11. 07:15 | Tue 09.11. 13:20 | Fri 12.11. 09:05 | Wed 10.11. 17:35 |
| Ticks in the 5 min | 3136 | 24600 | 99696 | 116699 | 130515 |
| Throughput, ticks/s (avg / busiest second) | 10 / 159 | 82 / 1230 | 332 / 2584 | 389 / 2919 | 435 / 4247 |
| Window closes received / expected (bars) | 167 / 167 | 1230 / 1230 | 3011 / 3011 | 3174 / 3174 | 4236 / 4236 |
| BUY/SELL signals | 3 | 0 | 16 | 51 | 66 |
| Latency window end → shown, ms (median / slowest) | 22.0 / 272.0 | 162.0 / 162.0 | 165.0 / 165.0 | 71.0 / 71.0 | 33.0 / 33.0 |
| Latency window end → BUY/SELL shown, ms (median / slowest) | 22.0 / 22.0 | NA / NA | 165.0 / 165.0 | 71.0 / 71.0 | 33.0 / 33.0 |
| System latency, closing tick sent → shown, ms (median / slowest) | 21.0 / 248.0 | 156.0 / 156.0 | 165.0 / 165.0 | 70.0 / 71.0 | 32.0 / 32.0 |
| Median ms: wait for closing tick / signal service / gateway + flush | 1.0 / 7.0 / 14.0 | 6.0 / 9.0 / 147.0 | 0.0 / 6.0 / 159.0 | 1.0 / 6.0 / 64.0 | 1.0 / 7.0 / 25.0 |

Latency is measured by a WebSocket client (bench/probe) that subscribes to every symbol, like the UI does for one; median and slowest are over the bars of the window (one per symbol) or its signals. A window closes in one burst per partition, so its bars arrive in one or a few gateway frames: each column is essentially one closing event, and where it falls in the flush interval of the gateway (gateway + flush) moves it by up to that interval. A window closes only when its partition receives a record from after the window end, so the wait for the closing tick depends on how busy the market is, not on the system. For latency distributions over many closes, see the load sweep.
