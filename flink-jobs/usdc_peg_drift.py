"""USDC peg drift detector: alert when Pyth USDC/USD deviates > threshold from $1.00.

Every Pyth price update is checked. Updates within the normal peg band are silently
dropped. Deviations emit a severity-tagged alert to the alerts topic.

Input:  pyth.usdc_usd.prices  (Avro, Schema Registry)
Output: alerts.usdc_peg        (JSON)

Severity tiers:
  INFO     deviation > 0.1%   (default threshold)
  WARNING  deviation > 0.5%
  CRITICAL deviation > 1.0%
"""

from __future__ import annotations

import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://redpanda:8081")
THRESHOLD = float(os.getenv("PEG_DRIFT_THRESHOLD", "0.001"))  # 0.1%


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)

    # ── Source ──────────────────────────────────────────────────────────────
    # price_raw and expo encode the actual price: actual = price_raw * 10^expo
    # e.g. price_raw=99975000, expo=-8 → $0.99975
    t_env.execute_sql(f"""
        CREATE TABLE pyth_usdc (
            feed_id       STRING,
            price_raw     BIGINT,
            conf_raw      BIGINT,
            expo          INT,
            publish_time  BIGINT,
            ema_price_raw BIGINT,
            ema_conf_raw  BIGINT
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'pyth.usdc_usd.prices',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'properties.group.id'          = 'flink-peg-drift',
            'scan.startup.mode'            = 'latest-offset',
            'format'                       = 'avro-confluent',
            'avro-confluent.url'           = '{SCHEMA_REGISTRY}'
        )
    """)

    # ── Sink ────────────────────────────────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE usdc_peg_alerts (
            publish_time  BIGINT,
            feed_id       STRING,
            usdc_usd      DOUBLE,
            deviation     DOUBLE,
            severity      STRING
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'alerts.usdc_peg',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'format'                       = 'json'
        )
    """)

    # ── Streaming query ──────────────────────────────────────────────────────
    # CAST to DOUBLE before POWER to avoid integer truncation.
    t_env.execute_sql(f"""
        INSERT INTO usdc_peg_alerts
        SELECT
            publish_time,
            feed_id,
            CAST(price_raw AS DOUBLE) * POWER(10.0, expo)                      AS usdc_usd,
            ABS(1.0 - CAST(price_raw AS DOUBLE) * POWER(10.0, expo))           AS deviation,
            CASE
                WHEN ABS(1.0 - CAST(price_raw AS DOUBLE) * POWER(10.0, expo)) > 0.01  THEN 'CRITICAL'
                WHEN ABS(1.0 - CAST(price_raw AS DOUBLE) * POWER(10.0, expo)) > 0.005 THEN 'WARNING'
                ELSE 'INFO'
            END                                                                AS severity
        FROM pyth_usdc
        WHERE ABS(1.0 - CAST(price_raw AS DOUBLE) * POWER(10.0, expo)) > {THRESHOLD}
    """).wait()


if __name__ == "__main__":
    main()
