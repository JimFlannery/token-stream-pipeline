# schemas

Avro schema definitions, versioned in Schema Registry starting Day 2.

## Schema files

| File | Subject (Schema Registry) | Used by |
|---|---|---|
| `trade_event.avsc` | `coinbase.{product}.trades-value` | `coinbase_trades.py` |
| `ticker_event.avsc` | `coinbase.{product}.ticker-value` | `coinbase_ticker.py` |
| `pyth_price.avsc` | `pyth.{feed}.prices-value` | `pyth_prices.py` |
| `ohlcv_bar.avsc` | `analytics.sol_usd.ohlcv_{window}-value` | ohlcv_bars Flink job (Day 3) |
| `cross_source_spread.avsc` | `analytics.sol.cross_source_spread-value` | cross_source_spread Flink job (Day 4) |

## Pyth price representation

`PythPriceEvent` stores the raw integer mantissa and exponent separately.
Actual price = `price_raw × 10^expo`.  
Example: `price_raw=8624000000, expo=-8` → `$86.24`.

This avoids floating-point imprecision in the broker; Flink casts to double when needed.

## Evolution policy

Backwards-compatible only. Add optional fields with defaults; never remove or rename fields.
Schema Registry is configured with `BACKWARD` compatibility (the default for Redpanda).
