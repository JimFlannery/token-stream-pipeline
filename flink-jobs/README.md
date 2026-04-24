# flink-jobs

PyFlink streaming jobs. Each job reads from one or more Kafka topics, does windowed processing, and writes back to Kafka.

## Jobs

| Job | Input | Output | Purpose |
|---|---|---|---|
| `ohlcv_bars.py` | `coinbase.sol_usd.trades` | `analytics.sol_usd.ohlcv_1s/10s/1m` | 1s/10s/1m tumbling window OHLCV bars |
| `usdc_peg_drift.py` | `pyth.usdc_usd.prices` | `alerts.usdc_peg` | Alert when USDC deviates from $1.00 |
| `cross_source_spread.py` | `coinbase.sol_usd.ticker` + `pyth.sol_usd.prices` | `analytics.sol.cross_source_spread` | CEX mid vs oracle; spread + rolling z-score. **Flagship.** |
| `order_book_imbalance.py` | `coinbase.sol_usd.ticker` | `analytics.sol_usd.imbalance` | HOP-windowed top-of-book bid/ask imbalance |

## Submit a job

```bash
# While Flink cluster is running:
docker compose exec flink-jobmanager flink run -py /opt/jobs/ohlcv_bars.py
docker compose exec flink-jobmanager flink run -py /opt/jobs/usdc_peg_drift.py
docker compose exec flink-jobmanager flink run -py /opt/jobs/cross_source_spread.py
docker compose exec flink-jobmanager flink run -py /opt/jobs/order_book_imbalance.py
```

## Design notes

- **OHLCV windowing**: uses `PROCTIME()` (processing-time). Appropriate for a live, near-zero-lag pipeline. For replay/backfill, switch to event-time via `TO_TIMESTAMP_LTZ` on the `time` field.
- **OHLCV open/close**: `FIRST_VALUE` / `LAST_VALUE` by arrival order ≈ correct for live data.
- **Peg drift**: stateless filter — no windowing needed; every Pyth update within the threshold is dropped silently.
- **Cross-source spread**: uses DataStream API (`CoProcessFunction`) rather than SQL because Flink drops time attributes after PROCTIME interval joins, making OVER windows unavailable. The `CoProcessFunction` maintains a Python list as a ring buffer (last 100 spreads) for a rolling z-score — Flink keyed state is unavailable without `keyBy()`, and PyFlink's Beam runner passes `(value, ctx)` to `process_element*`, so `yield` is used instead of `out.collect()`. Table API is still used for Avro source deserialization.
- **Order book imbalance**: uses a HOP (sliding) window — slide=5s, size=30s — producing output every 5 seconds. Contrast with TUMBLE (non-overlapping) used in ohlcv_bars. Reads from the existing `coinbase.sol_usd.ticker` topic (public channel, no auth needed); the Coinbase `level2` channel would give full-depth imbalance but requires API key authentication.
- **Avro deserialization**: requires `flink-avro-confluent-registry` JAR (included in the custom Flink image). Schema is fetched from `http://redpanda:8081` at job startup.
