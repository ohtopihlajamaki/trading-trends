# trading-trends
CS-E4780 Scalable Systems and Data Management Course Project Detecting Trading Trends in Financial Tick Data

The streaming pipeline (DuckDB → Redpanda → Go → ClickHouse → Vue) is in [duckdb-redpanda-go-clickhouse-vue/](duckdb-redpanda-go-clickhouse-vue/README.md); its design is [PLAN-2026-11-04.md](duckdb-redpanda-go-clickhouse-vue/plans/PLAN-2026-11-04.md).

## Running the system

### Prerequisites

- Docker with Compose v2 (`docker compose version` prints v2.x): Docker Desktop on macOS or Windows,
  or Docker Engine on Linux. Give Docker Desktop at least 8 GB of memory.
- bash. On Windows, run everything in WSL2 and keep the clone inside the WSL2 filesystem (not under `C:`).
- Disk: the CSV of each day you run (about 5 GB per day), plus about 10 GB for Docker images and data.
- Free ports: 5173, 8000, 8080, 8123, 9000, 9644 and 19092.

### Run

1. Put the day's CSV in `data/raw/` at the top of the repository. The name it has in the Zenodo download
   is fine (`debs2022-gc-trading-day-08-11-21.csv`); so is `081121.csv`. By default the run uses Monday,
   8 November 2021, so only that file is needed. If `debs2022-gc-trading-day-14-11-21.csv` is there too,
   it is used for the symbol details (index or equity, exchange); otherwise Monday's file is.
2. Run:

   ```sh
   cd duckdb-redpanda-go-clickhouse-vue
   ./run.sh
   ```

`run.sh` creates `.env` from `.env.example`, checks the CSV, builds and starts the services, preps the CSV
into Parquet, replays it into Redpanda at full speed, waits until every stage has caught up, and checks the
output against ClickHouse.

### Expected time and output

The first run takes 5–15 minutes, mostly to download the Docker images and build the services. After that
a run of one day takes about 2 minutes: prep about 45 s, replay about 15 s, and the signal service and
ClickHouse finish about 20 s later (MacBook Air M2).

It ends with:

```text
==> Checks: topics, bars, EMAs and signals against ClickHouse
...
  ok    ticks + watermarks = ticks topic offsets     11794552
  ok    bars_ema rows = bars_ema topic offsets       373126
  ok    signals rows = signals topic offsets         5735
...
  ok    signals with a different side                0
PASSED
```

For Monday that is 11,794,520 ticks, 373,126 five-minute bars and 5,735 BUY/SELL signals. `PASSED` means
every bar, EMA and signal the Go service computed equals what ClickHouse computes from the raw ticks
alone, exactly. Running `./run.sh` again repeats the checks without replaying.

### URLs

| URL | What |
| --- | --- |
| <http://localhost:5173> | UI: symbols, candles with EMAs and signals, live signal grid, throughput |
| <http://localhost:8080> | Redpanda Console: topics and consumer lag |
| <http://localhost:8000> | Gateway: `/symbols`, `/history?symbol=`, `/signals`, `/metrics` |
| <http://localhost:8123/play> | ClickHouse SQL console (user `app`, password `CLICKHOUSE_PASSWORD` in `.env`) |

The full-speed replay finishes in seconds, so the UI then shows the day as history. To watch signals arrive
live, replay a span more slowly into a fresh stack:

```sh
docker compose down -v && docker compose up -d --wait
docker compose --profile prep run --rm prep
docker compose --profile replay run --rm -e REPLAY_SPEED=100 -e REPLAY_FROM=09:00 replayer
```

### More to look at

- [Showcase](duckdb-redpanda-go-clickhouse-vue/showcase/README.md): follows one symbol from 12 raw CSV lines
  to a SELL marker in the browser, one pipeline stage per step, in a few minutes.
- Performance results, in [bench/results/](duckdb-redpanda-go-clickhouse-vue/bench/results/):
  [full-speed run](duckdb-redpanda-go-clickhouse-vue/bench/results/full-speed-Ohto-MacBook-Air-20261030-1055.md),
  [real-time latency](duckdb-redpanda-go-clickhouse-vue/bench/results/realtime-Ohto-MacBook-Air-20261030-1649.md),
  [load sweep](duckdb-redpanda-go-clickhouse-vue/bench/results/sweep-Ohto-MacBook-Air-20261030-1649.md) and
  [the whole week at 1000×](duckdb-redpanda-go-clickhouse-vue/bench/results/week-1000x-Ohto-MacBook-Air-20261106-1641.md).
- The [pipeline README](duckdb-redpanda-go-clickhouse-vue/README.md): the steps `run.sh` takes, more days,
  the layout, the checks and the benchmarks.

### Clean up

From `duckdb-redpanda-go-clickhouse-vue/`:

```sh
docker compose down        # stop the services, keep the topics and tables
docker compose down -v     # stop them and delete the topics and tables
```

The Parquet files prep writes stay in `data/ticks/`; delete that folder to remove them too.
