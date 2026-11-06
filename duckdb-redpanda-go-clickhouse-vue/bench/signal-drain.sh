#!/usr/bin/env bash
# Benchmark point for the signal stage: reset it, start K instances against
# the pre-filled ticks topic and time until the group's lag reaches zero.
# Usage: bench/signal-drain.sh K     (add -f compose.perf.yaml via COMPOSE_FILE)
set -euo pipefail
cd "$(dirname "$0")/.."
k=${1:?usage: $0 K}
. bench/lib.sh

bench/reset-signal.sh
records=$(docker compose exec -T redpanda rpk topic describe ticks -p | awk 'NR > 1 { s += $NF } END { print s }')
stats_start "signal-drain-k$k"
trap stats_stop EXIT
start=$(date +%s.%N)
docker compose up -d --scale signal="$k" signal >/dev/null 2>&1
# Drained when the committed offsets add up to the topic (TOTAL-LAG alone
# reads 0 before the group has committed anything)
committed=0
until (( committed == records )); do
  sleep 0.5
  committed=$(docker compose exec -T redpanda rpk group describe signal 2>/dev/null \
    | awk '$1 == "ticks" && $3 ~ /^[0-9]+$/ { s += $3 } END { print s + 0 }')
done
end=$(date +%s.%N)
awk -v k="$k" -v n="$records" -v s="$start" -v e="$end" \
  'BEGIN { printf "signal=%d drained %d records in %.2fs (%.0f records/s)\n", k, n, e - s, n / (e - s) }'
