#!/usr/bin/env bash
# Showcase step 03: the signal service reads step 02's demo ticks topic as a
# consumer group and writes bars and crossovers to demo bars_ema and signals
# topics; those are then read back into output/. Needs Docker and a .env (copy
# .env.example). Starts Redpanda if it is not running and stops it again at the
# end; ClickHouse is not started. The pipeline's own topics and consumer groups
# are not touched. Rewrites output/.
set -euo pipefail
cd "$(dirname "$0")/../.."
prev=showcase/02-replayer-to-topic/output
out=showcase/03-signal-to-bars-and-signals/output
ticks=showcase-ticks
bars=showcase-bars_ema
signals=showcase-signals
group=showcase-signal
container=showcase-signal
partitions=$(sed -n 's/^PARTITIONS=//p' .env)
partitions=${partitions:-32}

[ -f "$prev/partitions.txt" ] || {
  echo "error: $prev is missing; run showcase/02-replayer-to-topic/run.sh first" >&2
  exit 1
}

was_running=$(docker compose ps -q --status running redpanda)
docker compose up -d --wait redpanda
cleanup() {
  docker rm -f "$container" >/dev/null 2>&1 || true
  [ -n "$was_running" ] || docker compose stop redpanda
}
trap cleanup EXIT
rpk() { docker compose exec -T redpanda rpk "$@"; }

# The input is step 02's topic, unchanged since step 02 saved it. Compare
# partition, log start and high watermark: the leader epoch grows with every
# Redpanda restart.
offsets() { awk '{ print $1, $5, $6 }'; }
if ! diff -q <(rpk topic describe "$ticks" -p 2>/dev/null | offsets) <(offsets <"$prev/partitions.txt") >/dev/null; then
  echo "error: topic $ticks does not match $prev/partitions.txt; run showcase/02-replayer-to-topic/run.sh again" >&2
  exit 1
fi
records=$(wc -l <"$prev/records.jsonl" | tr -d ' ')

# Fresh output topics like redpanda/create-topics.sh makes, and a group with no commits
for t in "$bars" "$signals"; do
  rpk topic delete "$t" >/dev/null 2>&1 || true
  rpk topic create "$t" -p "$partitions" -r 1 -c retention.ms=-1 >/dev/null
done
rpk group delete "$group" >/dev/null 2>&1 || true

# The signal service as built from this checkout. Only the topics, the group
# and EMA_INIT differ from the compose service.
docker compose build --quiet signal
rm -rf "$out"
mkdir -p "$out"
docker rm -f "$container" >/dev/null 2>&1 || true
docker compose run -d --no-deps --name "$container" \
  --env TICKS_TOPIC="$ticks" --env BARS_TOPIC="$bars" --env SIGNALS_TOPIC="$signals" \
  --env SIGNAL_GROUP="$group" --env EMA_INIT=first_close \
  signal >/dev/null

# The service never exits: wait until the group has committed every record of
# the input. It commits a batch only after that batch's outputs are acknowledged.
committed=0
for _ in $(seq 60); do
  committed=$(rpk group describe "$group" -c 2>/dev/null |
    awk -v t="$ticks" '$1 == t && $3 ~ /^[0-9]+$/ { n += $3 } END { print n + 0 }')
  [ "$committed" -ge "$records" ] && break
  sleep 1
done
if [ "$committed" -lt "$records" ]; then
  docker logs "$container" >&2
  echo "error: the group committed $committed of $records records within 60 s" >&2
  exit 1
fi

# The service's own counters, then a clean stop (SIGTERM: it leaves the group)
docker exec "$container" wget -qO- http://127.0.0.1:9100/metrics |
  grep -E '^signal_(ticks|bars|signals|late_ticks|decode_errors|rebuild_records|assigned_partitions)' >"$out/metrics.txt"
docker stop "$container" >/dev/null
docker logs "$container" >"$out/signal.log" 2>&1
cat "$out/signal.log"

# Everything now in the output topics, partition by partition, in offset order
format='{"partition":%p,"offset":%o,"timestamp":%d,"key":"%k","value":%v}\n'
rpk topic consume "$bars" -o :end -f "$format" | sort -t: -k2,2n -k3,3n >"$out/bars_ema.jsonl"
rpk topic consume "$signals" -o :end -f "$format" | sort -t: -k2,2n -k3,3n >"$out/signals.jsonl"
rpk group describe "$group" -c >"$out/group-offsets.txt"
echo "$records records of $ticks committed by group $group;" \
  "$(wc -l <"$out/bars_ema.jsonl" | tr -d ' ') records in $bars," \
  "$(wc -l <"$out/signals.jsonl" | tr -d ' ') in $signals -> $out/"
