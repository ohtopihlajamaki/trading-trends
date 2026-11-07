# Real-time performance (20x, not real time)

bench, Ubuntu 26.04.1 LTS (kernel 7.0.0-43-generic), AMD Ryzen 5 5600 6-Core Processor, 12 cores; Docker with 12 CPUs and 11.1 GiB. Commit 3eef762-dirty, compose.yaml:compose.perf.yaml, gateway flush 250 ms, 2 signal instance(s).

| | p10 | p35 | p50 | p99 | p100 (busiest) |
|---|---|---|---|---|---|
| Window (Berlin time) | Mon 08.11. 21:10 | Thu 11.11. 07:15 | Tue 09.11. 13:20 | Fri 12.11. 09:05 | Wed 10.11. 17:35 |
| Ticks in the 5 min | 62720 | 492000 | 1993920 | 2333980 | 2610300 |
| Throughput, ticks/s (avg / busiest second) | 209 / 3180 | 1640 / 24600 | 6646 / 51680 | 7780 / 58380 | 8701 / 84940 |
| Window closes received / expected (bars) | 3340 / 3340 | 24600 / 24600 | 60220 / 60220 | 63480 / 63480 | 84720 / 84720 |
| BUY/SELL signals | 60 | 0 | 320 | 1020 | 1320 |
| Latency window end → shown, ms (median / slowest) | 20.0 / 204.0 | 158.0 / 158.0 | 342.0 / 342.0 | 333.0 / 333.0 | 480.0 / 480.0 |
| Latency window end → BUY/SELL shown, ms (median / slowest) | 20.0 / 204.0 | NA / NA | 342.0 / 342.0 | 333.0 / 333.0 | 190.0 / 480.0 |
| System latency, closing tick sent → shown, ms (median / slowest) | 19.0 / 203.0 | 152.0 / 152.0 | 341.0 / 342.0 | 332.0 / 333.0 | 479.0 / 480.0 |
| Median ms: wait for closing tick / signal service / gateway + flush | 1.0 / 9.0 / 12.0 | 6.0 / 25.0 / 119.0 | 1.0 / 39.0 / 297.0 | 1.0 / 26.0 / 299.0 | 1.0 / 39.0 / 419.0 |

Latency is measured by a WebSocket client (bench/probe) that subscribes to every symbol, like the UI does for one; median and slowest are over the bars of the window (one per symbol) or its signals. A window closes in one burst per partition, so its bars arrive in one or a few gateway frames: each column is essentially one closing event, and where it falls in the flush interval of the gateway (gateway + flush) moves it by up to that interval. A window closes only when its partition receives a record from after the window end, so the wait for the closing tick depends on how busy the market is, not on the system. For latency distributions over many closes, see the load sweep.
