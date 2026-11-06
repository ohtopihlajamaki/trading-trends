#!/usr/bin/env bash
# Showcase step 04: everything that ends in ClickHouse.
#   edge 8  the three demo topics -> Kafka engine tables -> tables, in database showcase
#   edge 3  prep -> instruments over HTTP, into database showcase
#   edge 9  step 01's Parquet buckets -> backfill.sql, into database showcase_backfill
# Needs Docker and a .env (copy .env.example). Starts Redpanda and ClickHouse if
# they are not running and stops them again at the end. The pipeline's own
# database (default), topics and consumer groups are not touched. Rewrites output/.
set -euo pipefail
cd "$(dirname "$0")/../.."
step=showcase/04-topics-to-clickhouse
out=$step/output
s1=showcase/01-raw-to-prep/output
s2=showcase/02-replayer-to-topic/output
s3=showcase/03-signal-to-bars-and-signals/output
db=showcase
backfill_db=showcase_backfill
groups=(showcase-ch-ticks showcase-ch-bars-ema showcase-ch-signals)

for f in "$s1/ticks" "$s2/partitions.txt" "$s3/bars_ema.jsonl"; do
  [ -e "$f" ] || { echo "error: $f is missing; run the steps before this one first" >&2; exit 1; }
done

running_rp=$(docker compose ps -q --status running redpanda)
running_ch=$(docker compose ps -q --status running clickhouse)
docker compose up -d --wait redpanda clickhouse
tmp=$(mktemp -d)
cleanup() {
  rm -rf "$tmp"
  docker compose exec -T clickhouse rm -rf /var/lib/clickhouse/user_files/showcase || true
  [ -n "$running_ch" ] || docker compose stop clickhouse
  [ -n "$running_rp" ] || docker compose stop redpanda
}
trap cleanup EXIT
rpk() { docker compose exec -T redpanda rpk "$@"; }
ch() {  # as clickhouse/check.sh does it
  docker compose exec -T clickhouse sh -c \
    'clickhouse client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" "$@"' -- "$@"
}
total() { awk 'NR > 1 { s += $NF } END { print s + 0 }'; }  # sum of high watermarks

# The inputs are the topics steps 02 and 03 left, unchanged since they saved them
offsets() { awk '{ print $1, $5, $6 }'; }
if ! diff -q <(rpk topic describe showcase-ticks -p 2>/dev/null | offsets) <(offsets <"$s2/partitions.txt") >/dev/null ||
  [ "$(rpk topic describe showcase-bars_ema -p 2>/dev/null | total)" != "$(wc -l <"$s3/bars_ema.jsonl" | tr -d ' ')" ] ||
  [ "$(rpk topic describe showcase-signals -p 2>/dev/null | total)" != "$(wc -l <"$s3/signals.jsonl" | tr -d ' ')" ]; then
  echo "error: the showcase topics do not match steps 02 and 03; run those steps again" >&2
  exit 1
fi
rm -rf "$out"
mkdir -p "$out/tables"

# --- Edge 8: topics -> ClickHouse --------------------------------------------
# A fresh database with the project's schema, and consumer groups with no commits
for d in "$db" "$backfill_db"; do
  ch -q "DROP DATABASE IF EXISTS $d SYNC"
  ch -q "CREATE DATABASE $d"
done
ch --database "$db" --multiquery --queries-file /sql/init/10-schema.sql
for g in "${groups[@]}"; do rpk group delete "$g" >/dev/null 2>&1 || true; done

# The Kafka engine tables come from the project's own 20-kafka.sh: run with a
# stand-in `clickhouse` that prints the SQL instead of executing it, then with
# the demo topics and groups swapped in, in the showcase database
printf '#!/bin/sh\nexec cat\n' >"$tmp/clickhouse"
chmod +x "$tmp/clickhouse"
consumers=$(sed -n 's/^CH_KAFKA_CONSUMERS=//p' .env)
PATH="$tmp:$PATH" CH_KAFKA_CONSUMERS="${consumers:-4}" KAFKA_BROKERS=redpanda:9092 \
  bash clickhouse/init/20-kafka.sh | grep -v '^kafka tables created' |
  sed -e "s/kafka_topic_list = '\([a-z_]*\)'/kafka_topic_list = 'showcase-\1'/" \
    -e "s/kafka_group_name = '\([a-z-]*\)'/kafka_group_name = 'showcase-\1'/" >"$out/kafka.sql"
[ "$(grep -c "kafka_topic_list = 'showcase-" "$out/kafka.sql")" = 3 ] || { echo "error: kafka.sql has no demo topics" >&2; exit 1; }
ch --database "$db" --multiquery <"$out/kafka.sql"

# Wait until every record of the three topics has arrived. ClickHouse flushes
# what its Kafka consumers read every few seconds
ticks=$(grep -c '"key":"[^"]' "$s2/records.jsonl")
watermarks=$(grep -c '"key":""' "$s2/records.jsonl")
want="$ticks $watermarks $(wc -l <"$s3/bars_ema.jsonl" | tr -d ' ') $(wc -l <"$s3/signals.jsonl" | tr -d ' ')"
have=""
for _ in $(seq 90); do
  have=$(ch --database "$db" -q "SELECT (SELECT count() FROM ticks), (SELECT count() FROM ticks_watermarks),
    (SELECT count() FROM bars_ema), (SELECT count() FROM signals)" | tr '\t' ' ')
  [ "$have" = "$want" ] && break
  sleep 1
done
[ "$have" = "$want" ] || { echo "error: ticks, watermarks, bars, signals in ClickHouse: $have, expected $want" >&2; exit 1; }
# Then detach the demo from Redpanda: the same DROPs 20-kafka.sh starts with
grep '^DROP' "$out/kafka.sql" | ch --database "$db" --multiquery
for g in "${groups[@]}"; do rpk group describe "$g" -c; echo; done >"$out/group-offsets.txt"

# --- Edge 3: prep -> instruments ---------------------------------------------
docker compose --profile prep build --quiet prep
docker compose --profile prep run --rm --no-deps -T \
  --user "$(id -u):$(id -g)" --env PYTHONUNBUFFERED=1 --env INSTRUMENT_DAYS=141121 \
  --env CLICKHOUSE_DATABASE="$db" --volume "$PWD/showcase:/showcase" --entrypoint python \
  prep /showcase/04-topics-to-clickhouse/instruments_snippet.py 2>&1 | grep -v '^ Container '

# --- Edge 9: Parquet buckets -> backfill.sql ---------------------------------
# backfill.sql reads data/ticks under ClickHouse's user_files; step 01's
# buckets are copied next to it and the path in the SQL is changed to match
docker compose exec -T clickhouse sh -c 'rm -rf /var/lib/clickhouse/user_files/showcase && mkdir -p /var/lib/clickhouse/user_files/showcase'
docker compose cp "$s1/ticks" clickhouse:/var/lib/clickhouse/user_files/showcase/ticks
ch --database "$backfill_db" --multiquery --queries-file /sql/init/10-schema.sql
sed "s#'data/ticks/bucket=#'showcase/ticks/bucket=#" clickhouse/backfill.sql >"$out/backfill.sql"
grep -q "'showcase/ticks/bucket=" "$out/backfill.sql" || { echo "error: backfill.sql path not found" >&2; exit 1; }
ch --database "$backfill_db" --multiquery <"$out/backfill.sql"

# --- What each database now holds --------------------------------------------
dump() {  # database, name, query
  ch --database "$1" -q "$3 FORMAT CSVWithNames" >"$out/tables/$1.$2.csv"
}
for d in "$db" "$backfill_db"; do
  dump "$d" instruments "SELECT * FROM instruments FINAL ORDER BY symbol"
  dump "$d" ticks "SELECT * FROM ticks ORDER BY symbol, event_ts, seq"
  dump "$d" ticks_watermarks "SELECT * FROM ticks_watermarks ORDER BY partition, offset"
  dump "$d" bars_5m "SELECT symbol, window_start, argMinMerge(open) AS open, max(high) AS high, min(low) AS low,
    argMaxMerge(close) AS close, sum(trades) AS trades FROM bars_5m GROUP BY symbol, window_start ORDER BY symbol, window_start"
  dump "$d" bars_ema "SELECT * FROM bars_ema FINAL ORDER BY symbol, window_start"
  dump "$d" signals "SELECT * FROM signals FINAL ORDER BY symbol, window_start"
  dump "$d" kafka_errors "SELECT * FROM kafka_errors ORDER BY topic, partition, offset"
done
# The oracle: signals computed from bars_5m alone (clickhouse/oracle.sql)
ch --database "$db" --queries-file /sql/oracle.sql --format CSVWithNames >"$out/tables/$db.oracle.csv"

{
  printf '%-18s %10s %18s\n' table "$db" "$backfill_db"
  for t in instruments ticks ticks_watermarks bars_5m bars_ema signals kafka_errors oracle; do
    a="$out/tables/$db.$t.csv" b="$out/tables/$backfill_db.$t.csv"
    printf '%-18s %10s %18s\n' "$t" "$(($(wc -l <"$a") - 1))" "$([ -f "$b" ] && echo $(($(wc -l <"$b") - 1)) || echo -)"
  done
} | tee "$out/counts.txt"
same() { cmp -s "$out/tables/$db.$1.csv" "$out/tables/$backfill_db.$1.csv" && echo identical || echo DIFFERENT; }
echo "ticks from the topic and from the backfill: $(same ticks); bars_5m: $(same bars_5m)"
