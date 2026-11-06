#!/usr/bin/env bash
# End-to-end run at full speed: replays DAYS (default Monday) at REPLAY_SPEED=max
# into a running pipeline and times when each stage has drained. Start from
# empty topics and tables (fresh volumes, or a separate COMPOSE_PROJECT_NAME),
# then run clickhouse/check.sh.
# Usage: bench/full-speed.sh
set -euo pipefail
cd "$(dirname "$0")/.."
. bench/lib.sh

stats_start full-speed
trap stats_stop EXIT
t0=$(now)
docker compose --profile replay run --rm --no-deps -e DAYS="${DAYS:-081121}" -e REPLAY_SPEED=max replayer 2>&1 | grep -E "replaying|done:"
t_rep=$(now)
total=$(offsets ticks)
echo "ticks topic records: $total"
t_sig=""; t_ch=""
while [[ -z "$t_sig" || -z "$t_ch" ]]; do
  [[ -z "$t_sig" ]] && (( $(committed signal ticks) == total )) && t_sig=$(now)
  if [[ -z "$t_ch" ]]; then
    n=$(ch -q "SELECT (SELECT count() FROM ticks) + (SELECT count() FROM ticks_watermarks)")
    (( n == total )) && t_ch=$(now)
  fi
  sleep 0.5
done
# Downstream outputs: wait until ClickHouse has every bars_ema/signals record
for topic in bars_ema signals; do
  want=$(offsets $topic)
  until (( $(ch -q "SELECT count() FROM $topic") == want )); do sleep 0.5; done
done
t_out=$(now)
awk -v t0="$t0" -v r="$t_rep" -v s="$t_sig" -v c="$t_ch" -v o="$t_out" -v n="$total" 'BEGIN {
  printf "replayer done      %7.2fs  (%.0f records/s)\n", r - t0, n / (r - t0)
  printf "signal drained     %7.2fs  (%.0f records/s)\n", s - t0, n / (s - t0)
  printf "ClickHouse ticks   %7.2fs  (%.0f records/s)\n", c - t0, n / (c - t0)
  printf "all outputs in CH  %7.2fs\n", o - t0 }'
