#!/usr/bin/env bash
# Build-order gates 3 and 4. Run from the host once a replay has drained:
#   3. ClickHouse holds every record of the ticks topic (ticks + watermarks).
#   4. The signal service's bars, EMAs and signals equal what ClickHouse
#      computes from the ticks alone (bars_5m and oracle.sql), exactly.
# Exits non-zero if any check fails.
set -euo pipefail
cd "$(dirname "$0")/.."

ch() {
  docker compose exec -T clickhouse sh -c \
    'clickhouse client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" "$@"' -- "$@"
}
offsets() {  # sum of high watermarks of a topic
  docker compose exec -T redpanda rpk topic describe "$1" -p | awk 'NR > 1 { s += $NF } END { print s + 0 }'
}

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

# A replay with REPLAY_COPIES=k adds copies c1:SYMBOL .. c<k-1>:SYMBOL of every
# symbol; each must have exactly its original's bars and signals
echo "Copies: every copy against its original"
copy_check() {  # table, columns that must be equal
  local table=$1 cond="" col copies rows orig bad
  for col in $2; do cond+=" OR c.$col != o.$col"; done
  read -r copies rows orig bad <<<"$(ch -q "
    SELECT uniqExact(extract(c.symbol, '^(c[0-9]+):')), count(),
           (SELECT count() FROM $table FINAL WHERE NOT match(symbol, '^c[0-9]+:')),
           countIf(o.symbol = ''$cond)
    FROM (SELECT *, replaceRegexpOne(symbol, '^c[0-9]+:', '') AS base
          FROM $table FINAL WHERE match(symbol, '^c[0-9]+:')) AS c
    LEFT JOIN (SELECT * FROM $table FINAL WHERE NOT match(symbol, '^c[0-9]+:')) AS o
      ON o.symbol = c.base AND o.window_start = c.window_start")"
  if (( copies == 0 )); then
    echo "  ..    no copies in $table"
    return
  fi
  check "$table: rows of $copies copies = $copies x the originals'" "$rows" "$((copies * orig))"
  check "$table: copy rows missing or different in the original" "$bad" 0
}
copy_check bars_ema "open high low close trades ema38 ema100"
copy_check signals "side close ema38 ema100"

if (( failed )); then
  echo "FAILED (largest EMA difference: $ema_max)"
  exit 1
fi
echo "PASSED"
