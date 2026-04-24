# producers

Python asyncio services that read from upstream sources and publish to Kafka topics.

## Planned producers

| Module | Source | Topic(s) |
|---|---|---|
| `coinbase_trades.py` | Coinbase Advanced Trade WS `market_trades` | `coinbase.sol_usd.trades` |
| `coinbase_ticker.py` | Coinbase Advanced Trade WS `ticker` | `coinbase.sol_usd.ticker`, `coinbase.sol_usdt.ticker` |
| `coinbase_level2.py` | Coinbase Advanced Trade WS `level2` | `coinbase.sol_usd.level2` |
| `pyth_prices.py` | Pyth Hermes SSE | `pyth.sol_usd.prices`, `pyth.usdc_usd.prices` |

## Why Coinbase, not Binance

`stream.binance.com` geoblocks the US (HTTP 451). Binance.US handshakes succeed for
`@trade`/`@aggTrade`/`@kline_*` streams but delivers zero frames — only quote streams
like `@bookTicker` actually push data. Coinbase Advanced Trade is the US-legal source
for real-time trades.

## Message format

Day 1: raw JSON for quick iteration.

Day 2+: Avro with Schema Registry. Schemas live in `../schemas/`.

## Run locally

```bash
uv run python -m producers.coinbase_trades
```
