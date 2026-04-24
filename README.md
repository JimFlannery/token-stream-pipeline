# token-stream-pipeline

Real-time crypto pricing pipeline: Binance WebSocket + Pyth Network oracle → Kafka (Redpanda) → PyFlink stream processing → ClickHouse → Grafana.

## Scope

Tracks SOL/USDC across one centralized exchange and one on-chain oracle, computing cross-source spread, rolling OHLCV bars, order book imbalance, and USDC peg drift — all in real time.

## Stack

| Layer | Technology |
|---|---|
| Broker | Redpanda (Kafka API) |
| Schema | Schema Registry + Avro |
| Producers | Python asyncio |
| Stream processing | PyFlink |
| Storage | ClickHouse |
| Visualization | Grafana |
| Orchestration | Docker Compose |
| CI | GitHub Actions |

## Data sources

- **Binance WebSocket** — `solusdt@aggTrade`, `solusdc@aggTrade`, `solusdt@bookTicker`, `solusdc@bookTicker`, `solusdt@depth@100ms`, `usdcusdt@bookTicker`
- **Pyth Hermes SSE** — SOL/USD, USDC/USD oracle prices

## Quick start

```bash
# day 1: broker only
docker compose up -d redpanda redpanda-console

# open Redpanda Console
open http://localhost:8080
```

See [ROADMAP.md](ROADMAP.md) for the day-by-day build plan, and [CLAUDE.md](CLAUDE.md) for context on design decisions.

## Architecture

```
Binance WS ─┐
            ├─► Python producers ─► Redpanda topics ─► PyFlink ─► ClickHouse ─► Grafana
Pyth Hermes ┘                             │
                                   Schema Registry
```

## Ports

| Service | Port |
|---|---|
| Redpanda Kafka (external) | 19092 |
| Redpanda Console | 8080 |
| Schema Registry | 8081 |
| Flink JobManager UI | 8082 |
| ClickHouse HTTP | 8123 |
| Grafana | 3000 |
