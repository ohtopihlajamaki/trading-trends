# duckdb-redpanda-go-clickhouse-vue

The streaming pipeline of the trading-trends course project. Commands below run from this directory.

A week of DEBS 2022 Grand Challenge tick data is replayed through **DuckDB prep → Redpanda → Go signal
service → ClickHouse → Go gateway → Vue UI**. The signal service computes EMA(38) and EMA(100) of every
symbol over 5-minute windows and emits BUY/SELL crossovers, which the browser shows live. Everything runs
in Docker Compose; only Docker and Git are needed on the host. [PLAN.md](../PLAN.md) is the design.

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
| `prep/` | DuckDB job: CSV → 32 Parquet buckets per day, sorted by event time; loads `instruments` |
| `replayer/` | Go: Parquet → `ticks`, bucket *k* → partition *k*, with a watermark record after each bucket-day |
| `signal/` | Go: windows, EMAs and the flip rule (`state.go`, unit-tested), consumer group with state rebuild |
| `gateway/` | Go: WebSocket fan-out of `bars_ema` and `signals`, history from ClickHouse |
| `ui/` | Vue 3 + Vite; Lightweight Charts, Perspective, uPlot |
| `clickhouse/` | Schema and Kafka tables (`init/`), `oracle.sql`, `check.sh`, `backfill.sql` |
| `redpanda/` | Topic creation |
| `observe/` | Prometheus and Grafana provisioning |
| `bench/` | `replay-speeds.sh` (producer speed sweep, results in `bench/results/`), `full-speed.sh` (time every stage of a full-speed replay), `reset-signal.sh`, `signal-drain.sh K` (time K signal instances draining the `ticks` topic) |

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

## Platform notes

- Windows: keep the clone (with its `data/`) inside the WSL2 filesystem; bind mounts from `C:` are very slow.
- Docker Desktop (macOS, Windows): give it at least 8 GB of memory.
- The gateway publishes port 8000 in the base setup, so `--scale gateway=M` needs `compose.perf.yaml`,
  which removes the port; the UI reaches every instance through the compose network.

## Data licence

DEBS 2022 Grand Challenge Trading Data © Infront Financial Technology GmbH, licensed under
[CC BY-NC-SA 4.0](http://creativecommons.org/licenses/by-nc-sa/4.0/). Source: <https://doi.org/10.5281/zenodo.6382482>.
