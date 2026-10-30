# Full-speed end-to-end run, 2026-10-30 10:55

Monday (081121) replayed at `REPLAY_SPEED=max` through the whole pipeline, on fresh volumes
(`COMPOSE_PROJECT_NAME=stock-signals-bench`), with `bench/full-speed.sh`.

**Not report-grade:** macOS base setup, so Redpanda runs in dev-container mode (no fsync) and nothing is
pinned. Report numbers come from Linux with `compose.perf.yaml`.

## Setup

| | |
| --- | --- |
| Host | Ohto-MacBook-Air (MacBook Air M2, Mac14,2, fanless), macOS 15.6.1, on battery |
| CPU, memory | Apple M2, 8 cores (4 performance + 4 efficiency), 16 GB |
| Docker | 28.4.0 (Docker Desktop), 8 CPUs, 7.7 GiB |
| Compose | `compose.yaml` only |
| Pipeline | 32 partitions, 4 replay workers, 1 signal instance, `REDPANDA_SMP=1`, `REDPANDA_MEMORY=1G`, 4 ClickHouse Kafka consumers |

## Throughput

11,794,552 records (11,794,520 ticks + 32 watermarks). Times are from the replayer's start.

| Stage | Done after | Records/s |
| --- | ---: | ---: |
| Replayer → Redpanda (producer's own timer) | 12.47 s | 946,176 |
| Replayer, including container start | 13.92 s | 847,127 |
| Signal service has committed every tick | 26.20 s | 450,174 |
| ClickHouse holds every tick | 29.06 s | 405,897 |
| ClickHouse holds every bar and signal | 31.44 s | — |

The bottleneck is the single signal instance: it consumes at about half the producer's rate, so ticks queue
in Redpanda. Next: scale the signal service (`bench/signal-drain.sh K`).

## Output and latency

- 373,126 bars, 5,735 signals (2,963 BUY, 2,772 SELL), 0 late ticks, 0 decode errors, 0 produce errors.
- Send-to-process latency in the signal service: mean 8.9 s, median between 8 and 16 s, max under 33 s.
  At full speed this is queueing behind the producer, not processing time. Batches took 31 ms on average
  (594 batches, 96% under 64 ms).

## Where the latency comes from (11:12–11:16)

Repeat runs on fresh volumes, sampling the replayer's and the signal service's metrics every ~0.5 s.
`signal_tick_latency_seconds` is the time from the replayer stamping `sent_ms` (just before `Produce`) to the
signal service decoding the tick, so it covers the producer buffer, the broker and the consumer's backlog.

| Run | Ticks/s sent | Mean latency | Distribution |
| --- | ---: | ---: | --- |
| max, 3 repeats | 0.91–1.02 M | 7.4–7.8 s | 0 under 256 ms, all under 16.4 s |
| 600× | 116 k (peaks ~220 k) | 7.7 ms | 84% ≤ 8 ms, 97% ≤ 16 ms, 99.8% ≤ 256 ms, max < 512 ms |

In the full-speed run the broker acknowledged ticks within 0.1–1.1 s, so almost all of the delay is the queue
in Redpanda in front of the signal service. That backlog grew to 7.0 M records when the replayer finished
(t ≈ 14 s), and the last ticks waited about 10–11 s:

| t | In Redpanda | Processed | Backlog |
| ---: | ---: | ---: | ---: |
| 2.0 s | 1.04 M | 0.12 M | 0.91 M |
| 7.0 s | 5.57 M | 1.83 M | 3.74 M |
| 14.1 s | 11.79 M | 4.75 M | 7.04 M |
| 18.7 s | 11.79 M | 7.29 M | 4.51 M |
| 23.9 s | 11.79 M | 11.79 M | 0 |

The signal instance processed about 390 k ticks/s while the replayer ran and about 720 k/s after it had
finished. On this 8-core laptop the replayer, Redpanda, ClickHouse and the signal service compete for the
same cores, so the consumer's rate at full speed reflects CPU contention, not only its own limit. At 600× the
backlog stayed below 14 k records and the latency stayed in milliseconds: the full-speed delay is queueing,
not processing time.

## Correctness

`clickhouse/check.sh` passed every check: topic offsets match the ClickHouse tables, no parse errors, the
bars equal `bars_5m`, the EMAs are bit-identical to the oracle's, and the signals equal `oracle.sql`.

## Repeat on mains power (11:24–11:26)

Same setup, laptop plugged in (charging from 33%). Three runs of `bench/full-speed.sh`, each on fresh volumes.

| Run | Replayer (own timer) | Signal drained | ClickHouse ticks | All outputs | Mean latency | Over 16.4 s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 14.34 s (822,592/s) | 29.97 s (393,480/s) | 32.27 s | 34.32 s | 10.52 s | 1,193,856 |
| 2 | 13.74 s (858,633/s) | 26.74 s (441,165/s) | 28.95 s | 30.08 s | 9.32 s | 286,743 |
| 3 | 11.94 s (988,143/s) | 25.03 s (471,254/s) | 27.86 s | 28.87 s | 8.40 s | 6,857 |

Batches averaged 30–33 ms (594–595 per run); 0 produce errors; `clickhouse/check.sh` passed on run 3.
Mains power did not make the runs faster than on battery (946,176 ticks/s, drained at 26.20 s): the spread
between runs (29–34 s end to end) is larger than any difference between battery and mains.
