# Whole week at 1000×, 2026-11-06 16:41

Functional check of the whole pipeline on all five trading days (081121–121121) at `REPLAY_SPEED=1000`, on fresh volumes (`COMPOSE_PROJECT_NAME=stock-signals-week`), after the step 1 instrumentation changes (`plans/scalability-plan.md`).

**Not report-grade:** MacBook Air M2, Docker Desktop with 8 CPUs and 7.7 GiB, `compose.yaml` only (Redpanda in dev-container mode, nothing pinned). The UI, Console, Prometheus and Grafana were started a few minutes into the replay, and one browser was connected.

## Setup

32 partitions, 4 replay workers, 1 signal instance, `EMA_INIT=first_close`, flip crossover rule, `REDPANDA_SMP=1`, 4 ClickHouse Kafka consumers.

## Throughput

| | |
| --- | ---: |
| Ticks + watermarks | 59,391,670 + 160 |
| Replay schedule | 406.8 s; took 406.8 s; max schedule lag 221 ms (3 min 41 s of market time) |
| Offered rate, averaged over the week incl. nights | 145,995 ticks/s |
| Signal service drained (from start) | 408.8 s, 0.4 s after the replayer finished |
| ClickHouse holds every tick | 414.9 s |
| ClickHouse holds every bar and signal | 415.4 s |
| Produce errors / decode errors / late ticks / rebuilds | 0 / 0 / 0 / 0 |

## Output

1,857,457 bars; 19,989 signals (10,523 BUY, 9,466 SELL).

## Latency

Signal service, replayer send → tick processed (`signal_tick_latency_seconds`, all 59.4M records):

| ≤ 8 ms | ≤ 16 ms | ≤ 64 ms | ≤ 256 ms | ≤ 512 ms | ≤ 1.024 s | mean |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 61.0% | 83.7% | 95.2% | 98.7% | 99.7% | 100% | 19 ms |

- Batches: 49,183, mean 2.1 ms (process + produce + commit).
- Gateway, closing tick sent → signal received (`gateway_e2e_latency_seconds`): mean 30 ms over 19,989 signals; 0 WebSocket messages dropped.

## Load per partition

`signal_ticks_total` by partition: min 1,344,022, max 2,822,994, max/mean **1.52**. This matches the prep-time bucket counts.

## Resources

`bench/results/stats-week-1000x-Ohto-MacBook-Air-20261106-1641.csv`, 200 samples per container during the replay (one about every 2 s). 100% = one CPU.

| Container | CPU mean | CPU max | Memory max |
| --- | ---: | ---: | ---: |
| clickhouse | 88.6% | 390.4% | 3,455 MiB |
| replayer | 36.1% | 119.9% | 825 MiB |
| signal (1 instance) | 24.0% | 79.8% | 134 MiB |
| redpanda | 20.4% | 59.0% | 995 MiB |
| gateway | 1.8% | 6.3% | 84 MiB |

At this rate no stage saturates. ClickHouse uses the most CPU, about 3.7× the signal service, because it ingests every tick and maintains `bars_5m`. The signal service stays below one core.

## Correctness

`clickhouse/check.sh` **PASSED** on the whole week:
- Topic offsets equal the ClickHouse rows, with 0 parse errors.
- All 1,857,457 bars equal `bars_5m`.
- The EMAs are bit-identical to `oracle.sql`.
- The 19,989 signals equal the oracle's, with none extra, none missing and none with a different side.
