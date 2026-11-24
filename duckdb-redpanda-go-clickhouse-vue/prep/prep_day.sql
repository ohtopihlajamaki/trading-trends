-- Run by prep.py once per day, which fills in the ${...} placeholders.
-- ${source} is the read_csv call defined in prep.py (read_csv()).

-- day: every row with a Last price and a Trading time. The feed only sends
-- fields that changed, so Trading date is empty on most rows (all index rows
-- among them); it is filled in with the file's day (${date}). seq keeps the
-- file (receipt) order, which breaks ties between equal event times.
CREATE OR REPLACE TABLE day AS
SELECT column00 AS symbol, column01 AS sectype,
       TRY_CAST(column21 AS DOUBLE) AS last,
       try_strptime(coalesce(column26, '${date}') || ' ' || column23, '%d-%m-%Y %H:%M:%S.%g')
           AT TIME ZONE 'Europe/Berlin' AS event_ts,
       row_number() OVER () AS seq,
       hash(column00) % ${partitions} AS bucket,
       column26 IS NULL AS date_filled
FROM ${source}
WHERE column00 != 'ID' AND column21 IS NOT NULL AND column23 IS NOT NULL;

-- One COPY per bucket, in receipt (file) order: ORDER BY seq keeps the raw
-- arrival order, and the signal service handles the disorder with
-- ALLOWED_LATENESS_MS. Last = 0 is a placeholder (the 00:00 snapshot before a
-- symbol's first trade, or a symbol that never trades), never a trade, so it
-- is rejected with the unparseable rows.
COPY (SELECT symbol, sectype, last, event_ts, seq FROM day
      WHERE bucket = ${bucket} AND event_ts IS NOT NULL AND last > 0
      ORDER BY seq)
TO '${out}' (FORMAT parquet, COMPRESSION zstd);
