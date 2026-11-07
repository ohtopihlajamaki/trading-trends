# Real-time performance (20x, not real time)

bench, Ubuntu 26.04.1 LTS (kernel 7.0.0-43-generic), AMD Ryzen 5 5600 6-Core Processor, 12 cores; Docker with 12 CPUs and 11.1 GiB. Commit 3eef762-dirty, compose.yaml:compose.perf.yaml, gateway flush 250 ms, 1 signal instance(s).

| | p10 | p35 | p50 | p99 | p100 (busiest) |
|---|---|---|---|---|---|
| Window (Berlin time) | Mon 08.11. 21:10 | Thu 11.11. 07:15 | Tue 09.11. 13:20 | Fri 12.11. 09:05 | Wed 10.11. 17:35 |
| Ticks in the 5 min | 31360 | 246000 | 996960 | 1166990 | 1305150 |
| Throughput, ticks/s (avg / busiest second) | 105 / 1590 | 820 / 12300 | 3323 / 25840 | 3890 / 29190 | 4350 / 42470 |
| Window closes received / expected (bars) | 1670 / 1670 | 12300 / 12300 | 30110 / 30110 | 31740 / 31740 | 42360 / 42360 |
| BUY/SELL signals | 30 | 0 | 160 | 510 | 660 |
| Latency window end → shown, ms (median / slowest) | 19.0 / 126.0 | 165.0 / 165.0 | 267.0 / 267.0 | 333.0 / 333.0 | 223.0 / 223.0 |
| Latency window end → BUY/SELL shown, ms (median / slowest) | 126.0 / 126.0 | NA / NA | 99.0 / 99.0 | 333.0 / 333.0 | 223.0 / 223.0 |
| System latency, closing tick sent → shown, ms (median / slowest) | 18.0 / 120.0 | 159.0 / 159.0 | 266.0 / 267.0 | 332.0 / 333.0 | 222.0 / 223.0 |
| Median ms: wait for closing tick / signal service / gateway + flush | 1.0 / 7.0 / 11.0 | 6.0 / 26.0 / 125.0 | 1.0 / 32.0 / 230.0 | 1.0 / 30.0 / 296.0 | 1.0 / 33.0 / 181.0 |

Latency is measured by a WebSocket client (bench/probe) that subscribes to every symbol, like the UI does for one; median and slowest are over the bars of the window (one per symbol) or its signals. A window closes in one burst per partition, so its bars arrive in one or a few gateway frames: each column is essentially one closing event, and where it falls in the flush interval of the gateway (gateway + flush) moves it by up to that interval. A window closes only when its partition receives a record from after the window end, so the wait for the closing tick depends on how busy the market is, not on the system. For latency distributions over many closes, see the load sweep.
