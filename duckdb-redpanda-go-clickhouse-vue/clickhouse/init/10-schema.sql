CREATE TABLE IF NOT EXISTS instruments (
    symbol LowCardinality(String), exchange LowCardinality(String),
    sectype LowCardinality(String), isin String, currency LowCardinality(String)
) ENGINE = ReplacingMergeTree ORDER BY symbol;

CREATE TABLE IF NOT EXISTS ticks (
    symbol   LowCardinality(String),
    sectype  LowCardinality(String),
    last     Float64 CODEC(Gorilla, ZSTD),
    event_ts DateTime64(3, 'Europe/Berlin') CODEC(DoubleDelta, ZSTD),
    seq      UInt64
) ENGINE = MergeTree PARTITION BY toDate(event_ts) ORDER BY (symbol, event_ts, seq);

CREATE TABLE IF NOT EXISTS bars_5m (   -- built from ticks, feeds the oracle
    symbol       LowCardinality(String),
    window_start DateTime('Europe/Berlin'),
    open   AggregateFunction(argMin, Float64, Tuple(DateTime64(3, 'Europe/Berlin'), UInt64)),
    high   SimpleAggregateFunction(max, Float64),
    low    SimpleAggregateFunction(min, Float64),
    close  AggregateFunction(argMax, Float64, Tuple(DateTime64(3, 'Europe/Berlin'), UInt64)),
    trades SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree ORDER BY (symbol, window_start);

CREATE MATERIALIZED VIEW IF NOT EXISTS bars_5m_mv TO bars_5m AS
SELECT symbol, toStartOfFiveMinutes(event_ts) AS window_start,
       argMinState(last, (event_ts, seq)) AS open, max(last) AS high, min(last) AS low,
       argMaxState(last, (event_ts, seq)) AS close, count() AS trades
FROM ticks GROUP BY symbol, window_start;

CREATE TABLE IF NOT EXISTS bars_ema (
    symbol LowCardinality(String), window_start DateTime('Europe/Berlin'),
    open Float64, high Float64, low Float64, close Float64,
    trades UInt32, ema38 Float64, ema100 Float64
) ENGINE = ReplacingMergeTree PARTITION BY toDate(window_start) ORDER BY (symbol, window_start);

CREATE TABLE IF NOT EXISTS signals (
    symbol LowCardinality(String), window_start DateTime('Europe/Berlin'),
    side Enum8('BUY' = 1, 'SELL' = 2),
    close Float64, ema38 Float64, ema100 Float64,
    emitted_at DateTime64(3, 'Europe/Berlin')
) ENGINE = ReplacingMergeTree ORDER BY (symbol, window_start);

-- Watermark records the replayer writes into ticks after each bucket-day
-- (empty symbol); kept so that ticks + watermarks = topic offsets can be checked
CREATE TABLE IF NOT EXISTS ticks_watermarks (
    partition UInt32, offset UInt64, watermark DateTime64(3, 'Europe/Berlin')
) ENGINE = MergeTree ORDER BY (partition, offset);

CREATE TABLE IF NOT EXISTS kafka_errors (
    topic LowCardinality(String), partition UInt32, offset UInt64, raw String, error String,
    received_at DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY (topic, partition, offset);
