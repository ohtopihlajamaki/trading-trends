-- Reference signals computed from bars_5m alone: first-close seeding and the
-- flip rule. The signal service must reproduce exactly this set (check.sh).
WITH 2 / 39 AS a38, 2 / 101 AS a100,
bars AS (
    SELECT symbol, window_start, argMaxMerge(close) AS close
    FROM bars_5m GROUP BY symbol, window_start
),
series AS (
    SELECT symbol,
           arraySort(groupArray(window_start))                              AS w,
           arrayMap(x -> x.2, arraySort(groupArray((window_start, close)))) AS c
    FROM bars GROUP BY symbol
),
ema AS (
    SELECT symbol, w,
           arrayFold((acc, x) -> arrayPushBack(acc,
                        (x * a38  + acc[-1].1 * (1 - a38),
                         x * a100 + acc[-1].2 * (1 - a100))),
                     arraySlice(c, 2), [(c[1], c[1])]) AS e
    FROM series
),
flat AS (
    SELECT symbol, w_i AS window_start, e_i.1 AS ema38, e_i.2 AS ema100,
           sign(e_i.1 - e_i.2) AS rel
    FROM ema ARRAY JOIN w AS w_i, e AS e_i
),
cmp AS (
    SELECT *,
           anyLast(nullIf(rel, 0)) OVER (PARTITION BY symbol ORDER BY window_start
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_rel
    FROM flat
)
SELECT symbol, window_start,
       multiIf(rel = 1 AND prev_rel = -1, 'BUY', rel = -1 AND prev_rel = 1, 'SELL', '') AS signal
FROM cmp WHERE signal != ''
ORDER BY symbol, window_start;
