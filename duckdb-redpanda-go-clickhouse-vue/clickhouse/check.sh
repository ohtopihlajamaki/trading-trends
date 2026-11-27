#!/usr/bin/env bash
# Build-order gates 3 and 4, run from the host after a replay:
#   3. ClickHouse holds every record of the ticks topic (ticks + watermarks).
#   4. The signal service's bars, EMAs and signals equal what ClickHouse
#      computes from the ticks alone (bars_5m and oracle.sql), exactly.
# It first waits until the replay has drained: no replayer is running and the
# signal service and ClickHouse have committed every record of their topics
# (at most DRAIN_TIMEOUT seconds, default 1800; DRAIN_TIMEOUT=0 skips the wait).
# Exits non-zero if any check fails.
set -euo pipefail
cd "$(dirname "$0")/.."
. bench/lib.sh

# group:topic pairs, in pipeline order, so the outputs are counted after the
# signal service has read every tick
consumers="signal:ticks ch-ticks:ticks ch-bars-ema:bars_ema ch-signals:signals"
lag() {  # records the consumers have yet to commit, as "group N ..." for those behind
  local pair group topic behind
  for pair in $consumers; do
    group=${pair%%:*} topic=${pair#*:}
    behind=$(( $(offsets "$topic") - $(committed "$group" "$topic") ))
    (( behind > 0 )) && printf '%s %d  ' "$group" "$behind"
  done
  return 0
}
replayer_running() {  # also lists 'docker compose run' containers
  [[ -n "$(docker compose ps -q --status running replayer 2>/dev/null)" ]]
}
wait_drained() {
  local timeout=${DRAIN_TIMEOUT:-1800} start behind last=0 quiet=0
  (( timeout > 0 )) || return 0
  start=$(date +%s)
  echo "Waiting until the pipeline has drained (at most ${timeout}s)"
  # Drained twice in a row, a second apart, so a pause in a replay is not taken for its end
  while (( quiet < 2 )); do
    if replayer_running; then behind="replayer still running"; else behind=$(lag); fi
    if [[ -z "$behind" ]]; then quiet=$((quiet + 1)); else quiet=0; fi
    if (( quiet < 2 )); then
      if (( $(date +%s) - start >= timeout )); then
        echo "  timed out after ${timeout}s; still behind: $behind" >&2
        exit 1
      fi
      if [[ -n "$behind" ]] && (( $(date +%s) - last >= 10 )); then
        echo "  behind: $behind"
        last=$(date +%s)
      fi
      sleep 1
    fi
  done
  echo "  drained after $(( $(date +%s) - start ))s"
}
wait_drained

failed=0
check() {  # name, value, expected
  if [[ "$2" == "$3" ]]; then
    printf '  ok    %-44s %s\n' "$1" "$2"
  else
    printf '  FAIL  %-44s %s (expected %s)\n' "$1" "$2" "$3"
    failed=1
  fi
}

echo "Gate 3: topics against ClickHouse"
ticks=$(ch -q "SELECT count() FROM ticks")
watermarks=$(ch -q "SELECT count() FROM ticks_watermarks")
check "ticks + watermarks = ticks topic offsets" "$((ticks + watermarks))" "$(offsets ticks)"
check "bars_ema rows = bars_ema topic offsets" "$(ch -q "SELECT count() FROM bars_ema")" "$(offsets bars_ema)"
check "signals rows = signals topic offsets" "$(ch -q "SELECT count() FROM signals")" "$(offsets signals)"
check "messages ClickHouse could not parse" "$(ch -q "SELECT count() FROM kafka_errors")" 0

echo "Gate 4: signal service against ClickHouse"
read -r only_ref only_svc diff <<<"$(ch -q "
  SELECT countIf(NOT in_svc), countIf(NOT in_ref),
         countIf(in_svc AND in_ref AND (s.open != r.open OR s.high != r.high OR s.low != r.low
                                        OR s.close != r.close OR s.trades != r.trades))
  FROM (SELECT symbol, window_start, open, high, low, close, trades, true AS in_svc FROM bars_ema FINAL) AS s
  FULL OUTER JOIN
       (SELECT symbol, window_start, argMinMerge(open) AS open, max(high) AS high, min(low) AS low,
               argMaxMerge(close) AS close, sum(trades) AS trades, true AS in_ref
        FROM bars_5m GROUP BY symbol, window_start) AS r
  USING (symbol, window_start)")"
check "bars only in bars_5m" "$only_ref" 0
check "bars only in bars_ema" "$only_svc" 0
check "bars with a different open/high/low/close/trades" "$diff" 0

# oracle.sql without its final SELECT: its CTEs, ending with cmp
oracle=$(sed 's/;[[:space:]]*$//' clickhouse/oracle.sql | grep -v '^--')
ctes=$(awk '/^SELECT /{exit} {print}' <<<"$oracle")
read -r ema_diff ema_max <<<"$(ch -q "
  $ctes
  SELECT countIf(s.ema38 != o.ema38 OR s.ema100 != o.ema100),
         max(greatest(abs(s.ema38 - o.ema38), abs(s.ema100 - o.ema100)))
  FROM (SELECT symbol, window_start, ema38, ema100 FROM bars_ema FINAL) AS s
  JOIN flat AS o USING (symbol, window_start)")"
check "EMAs not bit-identical to the oracle's" "$ema_diff" 0

read -r total only_oracle only_svc side <<<"$(ch -q "
  SELECT count(), countIf(NOT in_svc), countIf(NOT in_oracle), countIf(in_svc AND in_oracle AND o.signal != s.signal)
  FROM (SELECT symbol, window_start, signal, true AS in_oracle FROM ($oracle)) AS o
  FULL OUTER JOIN
       (SELECT symbol, window_start, toString(side) AS signal, true AS in_svc FROM signals FINAL) AS s
  USING (symbol, window_start)")"
echo "  ..    oracle and service signals, combined       $total"
check "signals only in oracle.sql" "$only_oracle" 0
check "signals only in the signal service" "$only_svc" 0
check "signals with a different side" "$side" 0

if (( failed )); then
  echo "FAILED (largest EMA difference: $ema_max)"
  exit 1
fi
echo "PASSED"
