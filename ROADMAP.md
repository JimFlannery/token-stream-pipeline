# Roadmap

Day-by-day plan. Check items off as they complete. The first unchecked item is always the next action.

## Day 1 — Broker up, first producer
- [x] `docker compose up -d redpanda redpanda-console` runs cleanly
- [x] Redpanda Console reachable at http://localhost:8080
- [x] `rpk` can create topic `coinbase.sol_usd.trades` from inside the container
- [x] Python producer connects to Coinbase Advanced Trade `market_trades` for SOL-USD and publishes JSON to `coinbase.sol_usd.trades`
- [x] `rpk topic consume coinbase.sol_usd.trades` shows live messages

## Day 2 — Schema Registry + more producers
- [x] Add Schema Registry to compose (built into Redpanda — verified at localhost:18081)
- [x] Write Avro schemas `TradeEvent`, `TickerEvent`, `PythPriceEvent` in `schemas/`
- [x] Convert Coinbase trades producer to Avro + Schema Registry
- [x] Add Coinbase `ticker` producer for SOL-USD and SOL-USDT → `coinbase.sol_usd.ticker`, `coinbase.sol_usdt.ticker`
- [x] Add Pyth Hermes SSE producer → `pyth.sol_usd.prices` and `pyth.usdc_usd.prices` (the USDC/USD feed doubles as the peg-drift source)

## Day 3 — PyFlink up + windowed aggregations
- [x] Build custom Flink image with Python + `apache-flink` installed
- [x] Submit first PyFlink job to the cluster
- [x] Job: 1s / 10s / 1m OHLCV bars from `coinbase.sol_usd.trades` → `analytics.sol_usd.ohlcv`
- [x] Job: USDC peg drift detector on `pyth.usdc_usd.prices` → `alerts.usdc_peg`

## Day 4 — Cross-source join (flagship job)
- [x] Flink job joining Coinbase SOL mid-price with Pyth SOL/USD oracle
- [x] Emit spread + rolling z-score to `analytics.sol.cross_source_spread`
- [x] Add order book imbalance job from Coinbase `level2` updates

## Day 5 — ClickHouse sink + materialized views
- [x] Add ClickHouse service to compose
- [x] Kafka engine tables consuming the analytics topics
- [x] Materialized views aggregating into queryable tables
- [x] Verify end-to-end: producer → topic → flink → topic → clickhouse table

## Day 6 — Grafana dashboards
- [x] Grafana service + ClickHouse datasource provisioned as code
- [x] Dashboards: live prices, cross-source spread, USDC peg drift, order book imbalance
- [x] Record a demo GIF

## Day 7 — Polish + publish
- [x] Architecture diagram in README
- [x] GitHub Actions: ruff + mypy + `docker compose config` smoke test
- [ ] Tag v0.1.0
- [ ] Push to public GitHub
- [ ] (Stretch) k8s manifests under `k8s/`
