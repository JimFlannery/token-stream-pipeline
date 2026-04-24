"""OHLCV bars: 1s / 10s / 1m tumbling windows over coinbase.sol_usd.trades.

Uses processing-time windowing (PROCTIME) — appropriate for a live, low-latency
pipeline where event time ≈ processing time. Switch to event-time watermarking
(parse `time` column) for replay / backfill scenarios.

Input:  coinbase.sol_usd.trades  (Avro, Schema Registry)
Output: analytics.sol_usd.ohlcv_1s / _10s / _1m  (JSON)
"""

from __future__ import annotations

import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://redpanda:8081")

# Window definitions: (Flink SQL interval literal, topic/table label)
WINDOWS = [
    ("INTERVAL '1' SECOND", "1s"),
    ("INTERVAL '10' SECOND", "10s"),
    ("INTERVAL '1' MINUTE", "1m"),
]


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)

    # ── Source ──────────────────────────────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE trades (
            product_id  STRING,
            price       DOUBLE,
            size        DOUBLE,
            proc_time   AS PROCTIME()
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'coinbase.sol_usd.trades',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'properties.group.id'          = 'flink-ohlcv',
            'scan.startup.mode'            = 'latest-offset',
            'format'                       = 'avro-confluent',
            'avro-confluent.url'           = '{SCHEMA_REGISTRY}'
        )
    """)

    # ── Sinks (one per window size) ──────────────────────────────────────────
    for _, label in WINDOWS:
        t_env.execute_sql(f"""
            CREATE TABLE ohlcv_{label} (
                window_start  TIMESTAMP(3),
                window_end    TIMESTAMP(3),
                product_id    STRING,
                `open`        DOUBLE,
                high          DOUBLE,
                low           DOUBLE,
                `close`       DOUBLE,
                volume        DOUBLE,
                trade_count   BIGINT
            ) WITH (
                'connector'                    = 'kafka',
                'topic'                        = 'analytics.sol_usd.ohlcv_{label}',
                'properties.bootstrap.servers' = '{BOOTSTRAP}',
                'format'                       = 'json'
            )
        """)

    # ── Three inserts submitted as one streaming job ─────────────────────────
    # StatementSet batches multiple DML statements into a single Flink job,
    # so they share the Kafka consumer and avoid re-reading the topic 3×.
    stmt_set = t_env.create_statement_set()
    for interval, label in WINDOWS:
        stmt_set.add_insert_sql(f"""
            INSERT INTO ohlcv_{label}
            SELECT
                TUMBLE_START(proc_time, {interval}) AS window_start,
                TUMBLE_END(proc_time,   {interval}) AS window_end,
                product_id,
                FIRST_VALUE(price)                  AS `open`,
                MAX(price)                          AS high,
                MIN(price)                          AS low,
                LAST_VALUE(price)                   AS `close`,
                SUM(size)                           AS volume,
                COUNT(*)                            AS trade_count
            FROM trades
            GROUP BY product_id, TUMBLE(proc_time, {interval})
        """)

    stmt_set.execute().wait()


if __name__ == "__main__":
    main()
