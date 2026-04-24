"""Cross-source spread: Coinbase SOL mid-price vs Pyth SOL/USD oracle.

Why DataStream API instead of pure SQL:
  Flink SQL OVER windows require a time attribute, but after a PROCTIME
  interval join the time attribute is dropped from the result. DataStream's
  CoProcessFunction avoids this by maintaining rolling state directly.

Architecture:
  - Table API defines both Kafka sources (handles Avro-Confluent deserialization).
  - DataStream CoProcessFunction joins the two streams with Python instance vars:
      stream 1 (coinbase_ticker) → compute spread, emit record
      stream 2 (pyth_sol)        → store latest price in self._pyth_price
  - z-score uses a plain Python list as a ring-buffer (last ZSCORE_WINDOW values).
  - Flink keyed state requires keyBy(); without it state returns None silently.
    Instance variables work correctly at parallelism=1 (single operator instance).
  - PyFlink Beam runner calls process_element1/2(value, ctx) — no 'out' arg.
    Use `yield` to emit; `out.collect()` is Java-style and not supported here.
  - Output: JSON strings → analytics.sol.cross_source_spread via KafkaSink.

Input:  coinbase.sol_usd.ticker  (Avro, Schema Registry)
        pyth.sol_usd.prices      (Avro, Schema Registry)
Output: analytics.sol.cross_source_spread (JSON)
"""

from __future__ import annotations

import json
import math
import os
import time

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaRecordSerializationSchema,
    KafkaSink,
)
from pyflink.datastream.functions import CoProcessFunction
from pyflink.table import StreamTableEnvironment

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://redpanda:8081")
ZSCORE_WINDOW = int(os.getenv("ZSCORE_WINDOW", "100"))  # rolling sample count
JOIN_TIMEOUT_S = int(os.getenv("JOIN_TIMEOUT_S", "10"))  # drop ticker if Pyth is stale


class SpreadFunction(CoProcessFunction):
    """Joins Coinbase ticker with Pyth oracle; emits spread + rolling z-score.

    stream 1 = coinbase_ticker rows  → drives output
    stream 2 = pyth_sol rows         → updates stored reference price

    Uses Python instance variables rather than Flink keyed state because
    connect() without keyBy() produces a non-keyed operator where get_state()
    returns None silently, making ValueState unusable.
    """

    def open(self, runtime_context) -> None:
        self._pyth_price: float | None = None
        self._pyth_ts: int | None = None
        self._spread_buf: list[float] = []

    def process_element1(self, ticker_row, ctx):
        # Ticker update: compute spread vs latest Pyth price, emit record.
        if self._pyth_price is None:
            return  # No Pyth price yet — wait.

        if self._pyth_ts is not None and (int(time.time()) - self._pyth_ts) > JOIN_TIMEOUT_S:
            return  # Pyth price is stale.

        coinbase_mid = (ticker_row["best_bid"] + ticker_row["best_ask"]) / 2.0
        spread = coinbase_mid - self._pyth_price

        self._spread_buf.append(spread)
        if len(self._spread_buf) > ZSCORE_WINDOW:
            self._spread_buf = self._spread_buf[-ZSCORE_WINDOW:]

        n = len(self._spread_buf)
        if n >= 2:
            mean = sum(self._spread_buf) / n
            variance = sum((x - mean) ** 2 for x in self._spread_buf) / n
            std = math.sqrt(variance)
            z_score = (spread - mean) / std if std > 0 else 0.0
        else:
            mean, std, z_score = spread, 0.0, 0.0

        yield json.dumps(
            {
                "ts_ms": int(time.time() * 1000),
                "coinbase_mid": round(coinbase_mid, 6),
                "pyth_price": round(self._pyth_price, 6),
                "spread": round(spread, 6),
                "rolling_mean": round(mean, 6),
                "rolling_std": round(std, 6),
                "z_score": round(z_score, 4),
                "sample_count": n,
            }
        )

    def process_element2(self, pyth_row, ctx):
        # Pyth update: decode and store for next ticker event.
        # actual_price = price_raw * 10^expo  (expo is negative, e.g. -8)
        self._pyth_price = float(pyth_row["price_raw"]) * (10.0 ** int(pyth_row["expo"]))
        self._pyth_ts = int(time.time())


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)

    # ── Sources (Table API handles Avro-Confluent deserialization) ───────────
    t_env.execute_sql(f"""
        CREATE TABLE coinbase_ticker (
            product_id  STRING,
            best_bid    DOUBLE,
            best_ask    DOUBLE
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'coinbase.sol_usd.ticker',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'properties.group.id'          = 'flink-spread-ticker',
            'scan.startup.mode'            = 'latest-offset',
            'format'                       = 'avro-confluent',
            'avro-confluent.url'           = '{SCHEMA_REGISTRY}'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE pyth_sol (
            feed_id       STRING,
            price_raw     BIGINT,
            conf_raw      BIGINT,
            expo          INT,
            publish_time  BIGINT,
            ema_price_raw BIGINT,
            ema_conf_raw  BIGINT
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'pyth.sol_usd.prices',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'properties.group.id'          = 'flink-spread-pyth',
            'scan.startup.mode'            = 'latest-offset',
            'format'                       = 'avro-confluent',
            'avro-confluent.url'           = '{SCHEMA_REGISTRY}'
        )
    """)

    # ── Convert to DataStream ────────────────────────────────────────────────
    # to_data_stream() produces DataStream[Row] with named fields.
    ticker_ds = t_env.to_data_stream(t_env.from_path("coinbase_ticker"))
    pyth_ds = t_env.to_data_stream(t_env.from_path("pyth_sol"))

    # ── Join + z-score via CoProcessFunction ─────────────────────────────────
    # ticker_ds = stream 1 (drives output)
    # pyth_ds   = stream 2 (updates reference price in state)
    spread_ds = ticker_ds.connect(pyth_ds).process(SpreadFunction(), output_type=Types.STRING())

    # ── Sink: JSON → Kafka ───────────────────────────────────────────────────
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("analytics.sol.cross_source_spread")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    spread_ds.sink_to(sink)

    # execute_async() submits the job and returns immediately — no blocking,
    # no Ctrl+C needed. Contrast with execute() which blocks and cancels the
    # cluster job when the Python process is interrupted.
    env.execute_async("cross_source_spread")


if __name__ == "__main__":
    main()
