#!/usr/bin/env bash
# Runs the whole pipeline on the days in DAYS (.env; Monday 081121 by default)
# and checks its output:
#   1. creates .env from .env.example if there is none
#   2. checks that each day's CSV is in DATA_DIR/raw (../data/raw), as
#      ddmmyy.csv or under its Zenodo name, debs2022-gc-trading-day-dd-mm-yy.csv
#   3. builds and starts Redpanda, ClickHouse, the signal service, the gateway
#      and the UI, and waits until they are healthy
#   4. preps the CSVs into Parquet buckets and loads the instruments table
#   5. replays the Parquet buckets into the ticks topic, unless that topic
#      already holds records (a second replay would append the days again)
#   6. waits until the pipeline has drained and runs clickhouse/check.sh
#   7. prints the URLs
# Running it again re-runs the checks on what is there. To start over:
#   docker compose down -v
# Usage: ./run.sh   (DAYS="081121 091121" ./run.sh overrides .env)
set -euo pipefail
cd "$(dirname "$0")"

step() { printf '\n==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

docker info >/dev/null 2>&1 || die "Docker is not running (start Docker Desktop or the Docker daemon)"
docker compose version >/dev/null 2>&1 || die "Docker Compose v2 is needed ('docker compose', not 'docker-compose')"

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example"
fi
# Values from the environment win over .env, as they do for Compose
env_value() { [[ -n "${!1:-}" ]] && echo "${!1}" || (set -a; . ./.env; echo "${!1:-}"); }
days=$(env_value DAYS)
data_dir=$(env_value DATA_DIR)
[[ -n "$days" ]] || die "DAYS is empty in .env"
[[ -n "$data_dir" ]] || die "DATA_DIR is empty in .env"

step "Input: days $days in $data_dir/raw"
missing=0
for day in $days; do
  zenodo="debs2022-gc-trading-day-${day:0:2}-${day:2:2}-${day:4:2}.csv"
  if [[ -f "$data_dir/raw/$day.csv" ]]; then
    echo "  $day: $data_dir/raw/$day.csv"
  elif [[ -f "$data_dir/raw/$zenodo" ]]; then
    echo "  $day: $data_dir/raw/$zenodo"
  else
    echo "  $day: missing; put $zenodo (or $day.csv) in $data_dir/raw/" >&2
    [[ -f "$data_dir/$zenodo" ]] && echo "        (it is in $data_dir/ itself; move it into $data_dir/raw/)" >&2
    missing=1
  fi
done
(( missing == 0 )) || die "input CSVs missing"

step "Building and starting the services (the first build takes a few minutes)"
docker compose up -d --build --wait redpanda clickhouse signal gateway console
# The UI starts with npm ci, which can take a minute on a fresh volume; it is
# not waited for here
docker compose up -d ui

. bench/lib.sh

step "Prep: CSV -> Parquet buckets (about a minute per day)"
docker compose --profile prep run --rm --build prep

step "Replay: Parquet -> ticks topic"
records=$(offsets ticks)
if (( records > 0 )); then
  echo "  The ticks topic already holds $records records, so the replay is skipped."
  echo "  To replay again, start over with: docker compose down -v && ./run.sh"
else
  docker compose --profile replay run --rm --build replayer
fi

step "Checks: topics, bars, EMAs and signals against ClickHouse"
clickhouse/check.sh

ui_note=" (still starting: docker compose logs -f ui)"
for _ in $(seq 1 120); do
  curl -fsS -o /dev/null http://localhost:5173 2>/dev/null && { ui_note=""; break; }
  sleep 1
done
step "Done"
cat <<EOF
  http://localhost:5173        UI: symbols, candles with EMAs and signals, live signal grid$ui_note
  http://localhost:8080        Redpanda Console: topics and consumer lag
  http://localhost:8000        Gateway: /symbols, /history?symbol=, /signals, /metrics
  http://localhost:8123/play   ClickHouse (user $(env_value CLICKHOUSE_USER), password CLICKHOUSE_PASSWORD in .env)

Stop with 'docker compose down' (keeps the data) or 'docker compose down -v' (deletes it).
EOF
