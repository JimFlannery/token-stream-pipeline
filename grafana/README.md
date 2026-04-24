# grafana

Grafana provisioning (datasources + dashboards as code). Added day 6.

## Planned dashboards

1. **Live prices** — SOL/USDT and SOL/USDC candlesticks from `analytics.solusdt.ohlcv_1s`.
2. **Cross-source spread** — Binance vs Pyth SOL price with spread + z-score (flagship).
3. **USDC peg drift** — rolling mid-price of USDCUSDT with peg-deviation alert line.
4. **Order book imbalance** — rolling imbalance metric + heatmap of book depth.

## Layout

```
grafana/
  provisioning/
    datasources/
      clickhouse.yaml
    dashboards/
      dashboards.yaml          # provider config
  dashboards/
    live_prices.json
    cross_source_spread.json
    usdc_peg.json
    order_book_imbalance.json
```
