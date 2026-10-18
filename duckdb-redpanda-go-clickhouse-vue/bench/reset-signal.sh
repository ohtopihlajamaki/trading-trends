#!/usr/bin/env bash
# Puts the signal stage back to "nothing processed": stops the signal service,
# forgets its offsets, empties bars_ema and signals (topics and ClickHouse
# tables). The ticks topic and ClickHouse's ticks are kept, so the next start
# of the signal service drains a pre-filled topic.
set -euo pipefail
cd "$(dirname "$0")/.."

ch() {
  docker compose exec -T clickhouse sh -c \
    'clickhouse client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" "$@"' -- "$@"
}
rpk() { docker compose exec -T redpanda rpk "$@"; }

docker compose stop signal >/dev/null 2>&1 || true
docker compose rm -f signal >/dev/null 2>&1 || true

# ClickHouse's consumers leave their groups when the Kafka tables go
ch --multiquery -q "
  DROP VIEW IF EXISTS bars_ema_queue_mv; DROP VIEW IF EXISTS kafka_errors_bars_ema_queue_mv;
  DROP VIEW IF EXISTS signals_queue_mv; DROP VIEW IF EXISTS kafka_errors_signals_queue_mv;
  DROP TABLE IF EXISTS bars_ema_queue; DROP TABLE IF EXISTS signals_queue;
  TRUNCATE TABLE bars_ema; TRUNCATE TABLE signals;"
for group in signal ch-bars-ema ch-signals; do
  rpk group delete "$group" >/dev/null 2>&1 || true
done
rpk topic delete bars_ema signals >/dev/null
docker compose run --rm redpanda-init >/dev/null
# Running gateways still fetch the deleted topics' IDs; a restart rereads them.
# Wait until they are back in their groups, or they would miss the next run.
if [[ -n "$(docker compose ps -q gateway)" ]]; then
  docker compose restart gateway >/dev/null
  until [[ -z "$(docker compose ps gateway --format '{{.Health}}' | grep -v '^healthy$')" ]]; do sleep 1; done
fi
# Re-creates all Kafka tables; ch-ticks keeps its offsets, so ticks are not re-read
docker compose exec -T clickhouse bash /docker-entrypoint-initdb.d/20-kafka.sh >/dev/null
echo "signal stage reset"
