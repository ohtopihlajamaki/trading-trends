# Real-time performance (20x, not real time)

bench, Ubuntu 26.04.1 LTS (kernel 7.0.0-43-generic), AMD Ryzen 5 5600 6-Core Processor, 12 cores; Docker with 12 CPUs and 11.1 GiB. Commit 3eef762-dirty, compose.yaml:compose.perf.yaml, gateway flush 250 ms, 1 signal instance(s).

| | p10 | p35 | p50 | p99 | p100 (busiest) |
|---|---|---|---|---|---|
| Window (Berlin time) | Mon 08.11. 21:10 | Thu 11.11. 07:15 | Tue 09.11. 13:20 | Fri 12.11. 09:05 | Wed 10.11. 17:35 |
| Ticks in the 5 min | 15680 | 123000 | 498480 | 583495 | 652575 |
| Throughput, ticks/s (avg / busiest second) | 52 / 795 | 410 / 6150 | 1662 / 12920 | 1945 / 14595 | 2175 / 21235 |
| Window closes received / expected (bars) | 835 / 835 | 6150 / 6150 | 15055 / 15055 | 15870 / 15870 | 21180 / 21180 |
| BUY/SELL signals | 15 | 0 | 80 | 255 | 330 |
| Latency window end → shown, ms (median / slowest) | 14.0 / 144.0 | 44.0 / 173.0 | 185.0 / 185.0 | 88.0 / 88.0 | 166.0 / 166.0 |
| Latency window end → BUY/SELL shown, ms (median / slowest) | 14.0 / 14.0 | NA / NA | 185.0 / 185.0 | 54.0 / 54.0 | 166.0 / 166.0 |
| System latency, closing tick sent → shown, ms (median / slowest) | 13.0 / 120.0 | 39.0 / 168.0 | 185.0 / 185.0 | 88.0 / 89.0 | 166.0 / 166.0 |
| Median ms: wait for closing tick / signal service / gateway + flush | 1.0 / 7.0 / 5.0 | 5.0 / 15.0 / 26.0 | 0.0 / 15.0 / 169.0 | 0.0 / 17.0 / 65.0 | 0.0 / 22.0 / 143.0 |

Latency is measured by a WebSocket client (bench/probe) that subscribes to every symbol, like the UI does for one; median and slowest are over the bars of the window (one per symbol) or its signals. A window closes in one burst per partition, so its bars arrive in one or a few gateway frames: each column is essentially one closing event, and where it falls in the flush interval of the gateway (gateway + flush) moves it by up to that interval. A window closes only when its partition receives a record from after the window end, so the wait for the closing tick depends on how busy the market is, not on the system. For latency distributions over many closes, see the load sweep.
