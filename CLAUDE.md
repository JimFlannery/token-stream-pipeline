# Context for Claude sessions in this repo

## Project purpose

Resume-building / learning project. The **explicit goal** is maximum resume surface area across in-demand streaming technologies (Kafka, Flink, Schema Registry, ClickHouse, Grafana). Authentic usage of each tech matters more than abstraction cleanliness. Every technology in the stack is there because it's a recognizable keyword to hiring managers — do not swap any for a "simpler" alternative without confirming.

## User style

- Works fast, long days. "1 week" timelines are realistic.
- Prefers hands-on in VS Code with the Claude extension — watches code appear in real time to learn.
- Comfortable with Python (uv), Docker, the Solana/crypto domain.
- Wants to understand what's happening, not just receive finished code. Prefer small, explained increments over large drops.

## Stack decisions (already made — don't relitigate without asking)

- **Redpanda** over vanilla Kafka — Kafka-API compatible, no ZooKeeper, single binary.
- **PyFlink** over Kafka Streams / ksqlDB — Flink is the high-demand skill being targeted.
- **ClickHouse** over TimescaleDB / Postgres — columnar OLAP, trendy, ideal for time-series analytics.
- **Avro + Schema Registry** — demonstrates schema evolution discipline.
- **Two data sources**: Coinbase Advanced Trade WebSocket (CEX firehose) + Pyth Hermes (on-chain oracle). The cross-source comparison is the flagship Flink job — do not drop Pyth.
  - Originally Binance; `stream.binance.com` geoblocks US (HTTP 451) and Binance.US silently gates `@trade`/`@aggTrade`/`@kline_*` streams (quote-only streams like `@bookTicker` still work). US-legal real-time trade data requires Coinbase.

## Tracked assets

SOL-denominated against stablecoins. Coinbase products:
- `SOL-USD` — primary SOL book; Coinbase treats USDC 1:1 as USD deposit, so this is effectively the USDC book.
- `SOL-USDT` — Tether-denominated SOL book (for cross-stablecoin comparison).

Pyth feeds (see `.env.example`):
- `SOL/USD` — oracle for the cross-source spread Flink job.
- `USDC/USD` — oracle used as the USDC peg-drift source (replaces the original Binance `USDCUSDT` plan; Coinbase doesn't expose a USDC/USD market since it treats them 1:1 internally).

## Flagship Flink jobs

1. **OHLCV bars** — 1s / 10s / 1m tumbling windows over Coinbase `market_trades` per product.
2. **Cross-source spread** — join Coinbase SOL mid-price with Pyth SOL/USD oracle, emit spread + rolling z-score. This is the headline job.
3. **USDC peg drift** — monitor Pyth `USDC/USD` feed, alert on deviation > threshold.
4. **Order book imbalance** — aggregate Coinbase `level2` updates into rolling bid/ask imbalance.

## Conventions

- Python deps managed with `uv`.
- Each service directory (`producers/`, `flink-jobs/`, etc.) has a local README describing its inputs and outputs.
- Avro schemas live in `schemas/`, versioned in Schema Registry.
- Topic naming: `<source>.<symbol>.<message_type>`, e.g. `coinbase.sol_usd.trades`, `pyth.sol_usd.prices`. Symbols are lowercased, dashes replaced with underscores.

## Out of scope (for v1)

- Kubernetes manifests — stretch goal only after Compose is end-to-end.
- Production-grade ops (backpressure tuning, exactly-once tuning beyond Flink defaults).
- Historical backfill — pipeline is live-only.

## Where to start

Read `ROADMAP.md`. The first unchecked item is the next action.
