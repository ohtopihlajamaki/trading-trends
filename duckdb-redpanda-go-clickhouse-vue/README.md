# duckdb-redpanda-go-clickhouse-vue

The streaming pipeline of the trading-trends course project. Commands below run from this directory.

A week of DEBS 2022 Grand Challenge tick data is replayed through **DuckDB prep → Redpanda → Go signal
service → ClickHouse → Go gateway → Vue UI**. The signal service computes EMA(38) and EMA(100) of every
symbol over 5-minute windows and emits BUY/SELL crossovers, which the browser shows live. Everything runs
in Docker Compose; only Docker and Git are needed on the host. [PLAN-2026-11-04.md](plans/PLAN-2026-11-04.md) is the design.

## Quick start

1. Put the CSV files in `../data/raw/` (the repository's `data/`, shared with `utils/`) (`081121.csv` … `141121.csv`).
2. `cp .env.example .env` and set `CLICKHOUSE_PASSWORD`.
3. Run:

```sh
docker compose up -d                                # Redpanda, ClickHouse, signal, gateway, UI
docker compose --profile prep run --rm prep         # CSV -> ../data/ticks/bucket=KK/DDMMYY.parquet (≈1 min/day)
docker compose --profile replay run --rm replayer   # Parquet -> ticks topic
clickhouse/check.sh                                 # gates 3 and 4: counts and the oracle
```

Then open the UI at <http://localhost:5173>. To watch the producer on its own, see
[producer-demo.md](producer-demo.md). `DAYS` in `.env` picks the days (one day by default; the full
week on the Linux box). Replaying the same days twice appends them twice; start over with
`docker compose down -v`.

| URL | What |
| --- | --- |
| <http://localhost:5173> | UI: symbols, candles with EMAs and signals, live signal grid, throughput |
| <http://localhost:8080> | Redpanda Console: topics and consumer lag |
| <http://localhost:8000> | Gateway: `/ws`, `/history?symbol=`, `/signals`, `/symbols`, `/metrics` |
| <http://localhost:8123/play> | ClickHouse (user `app`, password from `.env`) |
| <http://localhost:3000> | Grafana, with `docker compose --profile observe up -d` (Prometheus on 9090) |

## Layout

| Path | Contents |
| --- | --- |
| `prep/` | DuckDB job: CSV → 32 Parquet buckets per day, in receipt (file) order; loads `instruments` |
| `replayer/` | Go: Parquet → `ticks`, bucket *k* → partition *k*, with a watermark record after each bucket-day |
| `signal/` | Go: windows, EMAs and the flip rule (`state.go`, unit-tested), consumer group with state rebuild |
| `gateway/` | Go: WebSocket fan-out of `bars_ema` and `signals`, history from ClickHouse |
| `ui/` | Vue 3 + Vite; Lightweight Charts, Perspective, uPlot |
| `clickhouse/` | Schema and Kafka tables (`init/`), `oracle.sql`, `check.sh`, `backfill.sql` |
| `redpanda/` | Topic creation |
| `observe/` | Prometheus and Grafana provisioning |
| `bench/` | `replay-speeds.sh` (producer speed sweep, results in `bench/results/`), `full-speed.sh` (time every stage of a full-speed replay), `realtime.sh` (performance test, below; its WebSocket client is `probe/`), `reset-signal.sh`, `signal-drain.sh K` (time K signal instances draining the `ticks` topic) |

## Checks

`clickhouse/check.sh` compares, exactly:

- ticks topic offsets with ClickHouse's `ticks` + watermark records, and the output topics with their tables;
- the service's bars (`bars_ema`) with the bars ClickHouse builds from `ticks` (`bars_5m`);
- the service's EMAs with the oracle's, bit for bit;
- the service's signals with `clickhouse/oracle.sql`.

Unit tests of the signal logic: `docker run --rm -v "$PWD/signal:/src" -w /src golang:1.27.2 go test ./...`

## Benchmarks

Numbers for the report come only from Linux with the performance override (no dev-container mode for
Redpanda, pinned cores from `CPUS_*` in `.env`):

```sh
export COMPOSE_FILE=compose.yaml:compose.perf.yaml
docker compose up -d
bench/signal-drain.sh 4        # reset the signal stage, start 4 instances, time the drain
```

Other knobs: `PREP_THREADS`, `REPLAY_WORKERS`, `REPLAY_THREADS`, `REDPANDA_SMP`, and `CH_KAFKA_CONSUMERS` (then re-run
`docker compose exec clickhouse bash /docker-entrypoint-initdb.d/20-kafka.sh`).

## Performance test

`bench/realtime.sh` measures how the pipeline performs on the week's own load and writes CSV and Markdown
tables, with a description of the machine and the git commit, to `bench/results/`:

1. **Real time.** Picks 5-minute windows by how many ticks they hold. The week has two levels: about 3k
   ticks per window outside Xetra hours and about 100k during them. So the default points are p10 (quiet),
   p35 (morning before Xetra opens), p50 (typical trading), p99 and p100 (the busiest window, the 17:35
   closing auction). Each window is replayed at 1×. The test reports the latency from the window's end to a
   UI client receiving its bars and BUY/SELL signals, split into three stages: the wait for the tick that
   closes the window, the signal service, and the gateway with its flush interval.
2. **Load sweep.** Replays the hour around the p50 and p100 windows at 10× to 3000× real time, and reports
   the highest speed whose p95 latency stays within 1 s.

Each run starts from empty volumes and fast-forwards the day up to the measured span, so the EMAs are warm.
It then waits until every service has caught up and replays the span while `bench/probe` records what the
gateway sends. The test uses its own Compose project (`stock-signals-rt`), so stop the usual stack first.

```sh
bench/realtime.sh                                   # both parts, all prepped days: about 75 min
MODE=sweep SIGNAL_INSTANCES="1 2 4" bench/realtime.sh   # scaling of the signal service
MODE=realtime REALTIME_SPEED=20 bench/realtime.sh   # 5-minute smoke test (not real time)
```

Needs Docker, bash and perl, and the days prepped (`DAYS` limits the test to some of them). Other knobs are
listed at the top of the script. Commit the result files so runs on different machines can be compared.

## Platform notes

- Windows: keep the clone (with its `data/`) inside the WSL2 filesystem; bind mounts from `C:` are very slow.
- Docker Desktop (macOS, Windows): give it at least 8 GB of memory.
- The gateway publishes port 8000 in the base setup, so `--scale gateway=M` needs `compose.perf.yaml`,
  which removes the port; the UI reaches every instance through the compose network.

## Data licence

DEBS 2022 Grand Challenge Trading Data © Infront Financial Technology GmbH, licensed under
[CC BY-NC-SA 4.0](http://creativecommons.org/licenses/by-nc-sa/4.0/). Source: <https://doi.org/10.5281/zenodo.6382482>.
