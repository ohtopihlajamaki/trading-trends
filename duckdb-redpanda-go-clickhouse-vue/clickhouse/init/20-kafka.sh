#!/usr/bin/env bash
# Kafka engine tables and the materialized views that move their rows into
# ticks, bars_ema and signals. Runs on ClickHouse's first start; to apply a new
# CH_KAFKA_CONSUMERS later, re-run it (it drops and re-creates only these):
#   docker compose exec clickhouse bash /docker-entrypoint-initdb.d/20-kafka.sh
# The consumer groups keep their offsets, so nothing is read twice.
#
# Floats are read as text and converted with toFloat64 under
# precise_float_parsing = 1. ClickHouse's JSON reader ignores that setting
# and can land one ULP off ("4.94" -> 4.9399999999999995), which is enough
# for the oracle's EMAs to differ from the signal service's.
set -euo pipefail

consumers=${CH_KAFKA_CONSUMERS:-4}
broker=${KAFKA_BROKERS:-redpanda:9092}

kafka_settings() {  # topic, group, consumers
  cat <<SQL
ENGINE = Kafka
SETTINGS kafka_broker_list = '$broker', kafka_topic_list = '$1',
         kafka_group_name = '$2', kafka_format = 'JSONEachRow',
         kafka_num_consumers = $3, kafka_thread_per_consumer = 1,
         kafka_handle_error_mode = 'stream',
         input_format_json_read_numbers_as_strings = 1, precise_float_parsing = 1
SQL
}

errors_view() {  # source table
  cat <<SQL
CREATE MATERIALIZED VIEW kafka_errors_$1_mv TO kafka_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset, _raw_message AS raw, _error AS error
FROM $1 WHERE length(_error) > 0;
SQL
}

clickhouse client --host 127.0.0.1 --user "${CLICKHOUSE_USER:-default}" \
  --password "${CLICKHOUSE_PASSWORD:-}" --multiquery <<SQL
DROP VIEW IF EXISTS ticks_queue_mv;
DROP VIEW IF EXISTS ticks_watermarks_mv;
DROP VIEW IF EXISTS bars_ema_queue_mv;
DROP VIEW IF EXISTS signals_queue_mv;
DROP VIEW IF EXISTS kafka_errors_ticks_queue_mv;
DROP VIEW IF EXISTS kafka_errors_bars_ema_queue_mv;
DROP VIEW IF EXISTS kafka_errors_signals_queue_mv;
DROP TABLE IF EXISTS ticks_queue;
DROP TABLE IF EXISTS bars_ema_queue;
DROP TABLE IF EXISTS signals_queue;

CREATE TABLE ticks_queue (symbol String, sectype String, last String, ts_ms Int64, seq UInt64, sent_ms Int64)
$(kafka_settings ticks ch-ticks "$consumers");

-- A record with an empty symbol is a watermark from the replayer, not a tick
CREATE MATERIALIZED VIEW ticks_queue_mv TO ticks AS
SELECT symbol, sectype, toFloat64(last) AS last,
       fromUnixTimestamp64Milli(ts_ms, 'Europe/Berlin') AS event_ts, seq
FROM ticks_queue WHERE length(_error) = 0 AND symbol != ''
SETTINGS precise_float_parsing = 1;

CREATE MATERIALIZED VIEW ticks_watermarks_mv TO ticks_watermarks AS
SELECT _partition AS partition, _offset AS offset, fromUnixTimestamp64Milli(ts_ms, 'Europe/Berlin') AS watermark
FROM ticks_queue WHERE length(_error) = 0 AND symbol = '';

$(errors_view ticks_queue)

CREATE TABLE bars_ema_queue (symbol String, window_start_ms Int64, open String, high String, low String,
                             close String, trades UInt32, ema38 String, ema100 String)
$(kafka_settings bars_ema ch-bars-ema 1);

CREATE MATERIALIZED VIEW bars_ema_queue_mv TO bars_ema AS
SELECT symbol, toDateTime(intDiv(window_start_ms, 1000), 'Europe/Berlin') AS window_start,
       toFloat64(open) AS open, toFloat64(high) AS high, toFloat64(low) AS low, toFloat64(close) AS close,
       trades, toFloat64(ema38) AS ema38, toFloat64(ema100) AS ema100
FROM bars_ema_queue WHERE length(_error) = 0
SETTINGS precise_float_parsing = 1;

$(errors_view bars_ema_queue)

CREATE TABLE signals_queue (symbol String, window_start_ms Int64, side String, close String,
                            ema38 String, ema100 String, emitted_ms Int64)
$(kafka_settings signals ch-signals 1);

CREATE MATERIALIZED VIEW signals_queue_mv TO signals AS
SELECT symbol, toDateTime(intDiv(window_start_ms, 1000), 'Europe/Berlin') AS window_start,
       CAST(side AS Enum8('BUY' = 1, 'SELL' = 2)) AS side,
       toFloat64(close) AS close, toFloat64(ema38) AS ema38, toFloat64(ema100) AS ema100,
       fromUnixTimestamp64Milli(emitted_ms, 'Europe/Berlin') AS emitted_at
FROM signals_queue WHERE length(_error) = 0
SETTINGS precise_float_parsing = 1;

$(errors_view signals_queue)
SQL
echo "kafka tables created, ticks consumers: $consumers"
