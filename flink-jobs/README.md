# flink-jobs

PyFlink streaming jobs. Each job reads from one or more Kafka topics, does windowed processing, and writes back to Kafka.

## Jobs

| Job | Input | Output | Purpose |
|---|---|---|---|
| `ohlcv_bars.py` | `coinbase.sol_usd.trades` | `analytics.sol_usd.ohlcv_1s/10s/1m` | 1s/10s/1m tumbling window OHLCV bars |
| `usdc_peg_drift.py` | `pyth.usdc_usd.prices` | `alerts.usdc_peg` | Alert when USDC deviates from $1.00 |
| `cross_source_spread.py` *(Day 4)* | `coinbase.sol_usd.ticker` + `pyth.sol_usd.prices` | `analytics.sol.cross_source_spread` | CEX mid vs oracle; spread + z-score. **Flagship.** |
| `order_book_imbalance.py` *(Day 4)* | `coinbase.sol_usd.level2` | `analytics.sol_usd.imbalance` | Rolling bid/ask imbalance metric |

## Submit a job

```bash
# While Flink cluster is running:
docker compose exec flink-jobmanager flink run -py /opt/jobs/ohlcv_bars.py
docker compose exec flink-jobmanager flink run -py /opt/jobs/usdc_peg_drift.py
```

## Design notes

- **OHLCV windowing**: uses `PROCTIME()` (processing-time). Appropriate for a live, near-zero-lag pipeline. For replay/backfill, switch to event-time via `TO_TIMESTAMP_LTZ` on the `time` field.
- **OHLCV open/close**: `FIRST_VALUE` / `LAST_VALUE` by arrival order ≈ correct for live data.
- **Peg drift**: stateless filter — no windowing needed; every Pyth update within the threshold is dropped silently.
- **Avro deserialization**: requires `flink-avro-confluent-registry` JAR (included in the custom Flink image). Schema is fetched from `http://redpanda:8081` at job startup.
