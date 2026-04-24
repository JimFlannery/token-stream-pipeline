"""Top-of-book imbalance: rolling bid/ask pressure from Coinbase ticker.

The ticker channel (public, no auth) includes best_bid_quantity and
best_ask_quantity on every update. This job computes top-of-book imbalance:

    imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty)   ∈ [-1, 1]

Positive → buy pressure, negative → sell pressure.

A HOP (sliding) window smooths the per-tick noise into a usable signal:
slide=5s, size=30s → emits every 5 s averaging 30 s of data.

Note: this is single-level (top-of-book) imbalance. The Coinbase level2
channel (full depth) requires API authentication, so ticker is used instead.

Input:  coinbase.sol_usd.ticker   (Avro, Schema Registry)
Output: analytics.sol_usd.imbalance (JSON)
"""

from __future__ import annotations

import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://redpanda:8081")


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)

    # ── Source ───────────────────────────────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE coinbase_ticker (
            product_id         STRING,
            best_bid           DOUBLE,
            best_ask           DOUBLE,
            best_bid_quantity  DOUBLE,
            best_ask_quantity  DOUBLE,
            proc_time          AS PROCTIME()
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'coinbase.sol_usd.ticker',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'properties.group.id'          = 'flink-imbalance',
            'scan.startup.mode'            = 'latest-offset',
            'format'                       = 'avro-confluent',
            'avro-confluent.url'           = '{SCHEMA_REGISTRY}'
        )
    """)

    # ── Sink ─────────────────────────────────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE imbalance_out (
            window_start   TIMESTAMP(3),
            window_end     TIMESTAMP(3),
            product_id     STRING,
            avg_imbalance  DOUBLE,
            avg_spread     DOUBLE,
            avg_bid_qty    DOUBLE,
            avg_ask_qty    DOUBLE,
            tick_count     BIGINT
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'analytics.sol_usd.imbalance',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'format'                       = 'json'
        )
    """)

    # ── Streaming query ───────────────────────────────────────────────────────
    # CASE guard prevents divide-by-zero if both quantities are ever zero.
    # HOP differs from TUMBLE: windows overlap (each row covers 30 s of history
    # but a new one starts every 5 s). Compare: ohlcv_bars uses TUMBLE.
    t_env.execute_sql("""
        INSERT INTO imbalance_out
        SELECT
            HOP_START(proc_time, INTERVAL '5' SECOND, INTERVAL '30' SECOND) AS window_start,
            HOP_END  (proc_time, INTERVAL '5' SECOND, INTERVAL '30' SECOND) AS window_end,
            product_id,
            AVG(
                CASE
                    WHEN best_bid_quantity + best_ask_quantity > 0
                    THEN (best_bid_quantity - best_ask_quantity)
                         / (best_bid_quantity + best_ask_quantity)
                    ELSE 0.0
                END
            )                        AS avg_imbalance,
            AVG(best_ask - best_bid) AS avg_spread,
            AVG(best_bid_quantity)   AS avg_bid_qty,
            AVG(best_ask_quantity)   AS avg_ask_qty,
            COUNT(*)                 AS tick_count
        FROM coinbase_ticker
        GROUP BY
            product_id,
            HOP(proc_time, INTERVAL '5' SECOND, INTERVAL '30' SECOND)
    """).wait()


if __name__ == "__main__":
    main()
