#!/usr/bin/env bash
# Showcase step 02: the replayer reads step 01's Parquet buckets and produces
# them to a demo topic on the project's Redpanda; the topic is then read back
# into output/. Needs Docker and a .env (copy .env.example). Starts Redpanda if
# it is not running and stops it again at the end; ClickHouse is not started.
# The pipeline's own ticks topic is not touched. Rewrites output/.
set -euo pipefail
cd "$(dirname "$0")/../.."
in=showcase/01-raw-to-prep/output/ticks
out=showcase/02-replayer-to-topic/output
topic=showcase-ticks
partitions=$(sed -n 's/^PARTITIONS=//p' .env)
partitions=${partitions:-32}

[ -d "$in" ] || { echo "error: $in is missing; run showcase/01-raw-to-prep/run.sh first" >&2; exit 1; }

was_running=$(docker compose ps -q --status running redpanda)
docker compose up -d --wait redpanda
[ -n "$was_running" ] || trap 'docker compose stop redpanda' EXIT
rpk() { docker compose exec -T redpanda rpk "$@"; }

# A fresh topic, made like the pipeline's topics in redpanda/create-topics.sh
rpk topic delete "$topic" >/dev/null 2>&1 || true
rpk topic create "$topic" -p "$partitions" -r 1 -c retention.ms=-1 >/dev/null

# The replayer as built from this checkout. Only the input directory, the topic
# and the replay settings differ from `docker compose --profile replay run replayer`.
docker compose --profile replay build --quiet replayer
rm -rf "$out"
mkdir -p "$out"
docker compose --profile replay run --rm --no-deps -T \
  --env TICKS_DIR=/showcase/ticks --env TICKS_TOPIC="$topic" --env DAYS=081121 \
  --env REPLAY_SPEED=max --env REPLAY_FROM= --env REPLAY_UNTIL= \
  --volume "$PWD/$in:/showcase/ticks:ro" \
  replayer 2>&1 | grep -v '^ Container ' | tee "$out/replayer.log"

# Everything now in the topic, partition by partition, in offset order
rpk topic consume "$topic" -o :end \
  -f '{"partition":%p,"offset":%o,"timestamp":%d,"key":"%k","value":%v}\n' |
  sort -t: -k2,2n -k3,3n >"$out/records.jsonl"
rpk topic describe "$topic" -p >"$out/partitions.txt"
echo "$(wc -l <"$out/records.jsonl" | tr -d ' ') records in $topic -> $out/records.jsonl, offsets in $out/partitions.txt"
