-- ClickHouse init: runs once at container startup.
-- Bind-mounted to /docker-entrypoint-initdb.d/init.sql (read-only).
--
-- Pattern per analytics topic:
--   1. Kafka engine table  — subscribes to the topic, deserialises JSON
--   2. MergeTree table     — queryable, persisted storage (Grafana points here)
--   3. Materialized view   — bridges 1 → 2; fires on every Kafka batch
--
-- All analytics topics are written by Flink in JSONEachRow format.
-- Timestamps from Flink TIMESTAMP(3) arrive as strings: "YYYY-MM-DD HH:MM:SS.ffffff"
-- DateTime64(6) in the Kafka table matches that precision exactly; the MergeTree
-- tables use DateTime64(3) (millisecond) since that is the underlying Flink precision.

CREATE DATABASE IF NOT EXISTS tsp;

-- ============================================================
-- OHLCV 1-second bars
-- ============================================================

CREATE TABLE IF NOT EXISTS tsp.kafka_ohlcv_1s
(
    window_start DateTime64(6),
    window_end   DateTime64(6),
    product_id   String,
    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    volume       Float64,
    trade_count  UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'redpanda:9092',
    kafka_topic_list           = 'analytics.sol_usd.ohlcv_1s',
    kafka_group_name           = 'ch-ohlcv-1s',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 1;

CREATE TABLE IF NOT EXISTS tsp.ohlcv_1s
(
    window_start DateTime64(3),
    product_id   LowCardinality(String),
    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    volume       Float64,
    trade_count  UInt64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (product_id, window_start);

CREATE MATERIALIZED VIEW IF NOT EXISTS tsp.ohlcv_1s_mv TO tsp.ohlcv_1s AS
SELECT
    toDateTime64(window_start, 3) AS window_start,
    product_id,
    open, high, low, close, volume, trade_count
FROM tsp.kafka_ohlcv_1s;

-- ============================================================
-- OHLCV 10-second bars
-- ============================================================

CREATE TABLE IF NOT EXISTS tsp.kafka_ohlcv_10s
(
    window_start DateTime64(6),
    window_end   DateTime64(6),
    product_id   String,
    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    volume       Float64,
    trade_count  UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'redpanda:9092',
    kafka_topic_list           = 'analytics.sol_usd.ohlcv_10s',
    kafka_group_name           = 'ch-ohlcv-10s',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 1;

CREATE TABLE IF NOT EXISTS tsp.ohlcv_10s
(
    window_start DateTime64(3),
    product_id   LowCardinality(String),
    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    volume       Float64,
    trade_count  UInt64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (product_id, window_start);

CREATE MATERIALIZED VIEW IF NOT EXISTS tsp.ohlcv_10s_mv TO tsp.ohlcv_10s AS
SELECT
    toDateTime64(window_start, 3) AS window_start,
    product_id,
    open, high, low, close, volume, trade_count
FROM tsp.kafka_ohlcv_10s;

-- ============================================================
-- OHLCV 1-minute bars
-- ============================================================

CREATE TABLE IF NOT EXISTS tsp.kafka_ohlcv_1m
(
    window_start DateTime64(6),
    window_end   DateTime64(6),
    product_id   String,
    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    volume       Float64,
    trade_count  UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'redpanda:9092',
    kafka_topic_list           = 'analytics.sol_usd.ohlcv_1m',
    kafka_group_name           = 'ch-ohlcv-1m',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 1;

CREATE TABLE IF NOT EXISTS tsp.ohlcv_1m
(
    window_start DateTime64(3),
    product_id   LowCardinality(String),
    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    volume       Float64,
    trade_count  UInt64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (product_id, window_start);

CREATE MATERIALIZED VIEW IF NOT EXISTS tsp.ohlcv_1m_mv TO tsp.ohlcv_1m AS
SELECT
    toDateTime64(window_start, 3) AS window_start,
    product_id,
    open, high, low, close, volume, trade_count
FROM tsp.kafka_ohlcv_1m;

-- ============================================================
-- Cross-source spread (Coinbase mid vs Pyth oracle)
-- ============================================================
-- ts_ms arrives as a JSON integer (epoch milliseconds from System.currentTimeMillis).

CREATE TABLE IF NOT EXISTS tsp.kafka_cross_source_spread
(
    ts_ms         Int64,
    coinbase_mid  Float64,
    pyth_price    Float64,
    spread        Float64,
    rolling_mean  Float64,
    rolling_std   Float64,
    z_score       Float64,
    sample_count  UInt32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'redpanda:9092',
    kafka_topic_list           = 'analytics.sol.cross_source_spread',
    kafka_group_name           = 'ch-spread',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 1;

CREATE TABLE IF NOT EXISTS tsp.cross_source_spread
(
    ts            DateTime64(3),
    coinbase_mid  Float64,
    pyth_price    Float64,
    spread        Float64,
    rolling_mean  Float64,
    rolling_std   Float64,
    z_score       Float64,
    sample_count  UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ts)
ORDER BY ts;

CREATE MATERIALIZED VIEW IF NOT EXISTS tsp.cross_source_spread_mv TO tsp.cross_source_spread AS
SELECT
    fromUnixTimestamp64Milli(ts_ms) AS ts,
    coinbase_mid,
    pyth_price,
    spread,
    rolling_mean,
    rolling_std,
    z_score,
    sample_count
FROM tsp.kafka_cross_source_spread;

-- ============================================================
-- Order book imbalance (top-of-book, HOP 5s/30s)
-- ============================================================

CREATE TABLE IF NOT EXISTS tsp.kafka_imbalance
(
    window_start  DateTime64(6),
    window_end    DateTime64(6),
    product_id    String,
    avg_imbalance Float64,
    avg_spread    Float64,
    avg_bid_qty   Float64,
    avg_ask_qty   Float64,
    tick_count    UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'redpanda:9092',
    kafka_topic_list           = 'analytics.sol_usd.imbalance',
    kafka_group_name           = 'ch-imbalance',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 1;

CREATE TABLE IF NOT EXISTS tsp.imbalance
(
    window_start   DateTime64(3),
    product_id     LowCardinality(String),
    avg_imbalance  Float64,
    avg_spread     Float64,
    avg_bid_qty    Float64,
    avg_ask_qty    Float64,
    tick_count     UInt64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (product_id, window_start);

CREATE MATERIALIZED VIEW IF NOT EXISTS tsp.imbalance_mv TO tsp.imbalance AS
SELECT
    toDateTime64(window_start, 3) AS window_start,
    product_id,
    avg_imbalance,
    avg_spread,
    avg_bid_qty,
    avg_ask_qty,
    tick_count
FROM tsp.kafka_imbalance;

-- ============================================================
-- USDC peg drift alerts
-- ============================================================
-- publish_time is a Unix timestamp in seconds from the Pyth attestation.

CREATE TABLE IF NOT EXISTS tsp.kafka_usdc_peg_alerts
(
    publish_time  Int64,
    feed_id       String,
    usdc_usd      Float64,
    deviation     Float64,
    severity      String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'redpanda:9092',
    kafka_topic_list           = 'alerts.usdc_peg',
    kafka_group_name           = 'ch-peg-alerts',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 1;

CREATE TABLE IF NOT EXISTS tsp.usdc_peg_alerts
(
    ts        DateTime,
    feed_id   String,
    usdc_usd  Float64,
    deviation Float64,
    severity  LowCardinality(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ts)
ORDER BY ts;

CREATE MATERIALIZED VIEW IF NOT EXISTS tsp.usdc_peg_alerts_mv TO tsp.usdc_peg_alerts AS
SELECT
    toDateTime(publish_time) AS ts,
    feed_id,
    usdc_usd,
    deviation,
    severity
FROM tsp.kafka_usdc_peg_alerts;
