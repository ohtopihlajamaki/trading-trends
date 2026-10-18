-- Loads ticks straight from the Parquet buckets, without Redpanda: for
-- history-only setups and for checking the Kafka path against the files.
-- Do not combine with a replay into the same tables (ticks would double).
--   docker compose exec clickhouse sh -c 'clickhouse client --user "$CLICKHOUSE_USER" \
--     --password "$CLICKHOUSE_PASSWORD" --queries-file /sql/backfill.sql'
INSERT INTO ticks (symbol, sectype, last, event_ts, seq)
SELECT symbol, sectype, last, toDateTime64(event_ts, 3, 'Europe/Berlin'), toUInt64(seq)
FROM file('data/ticks/bucket=*/*.parquet', Parquet,
          'symbol String, sectype String, last Float64, event_ts DateTime64(6, \'UTC\'), seq Int64');
