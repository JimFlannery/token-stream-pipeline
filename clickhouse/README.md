# clickhouse

ClickHouse schemas and init SQL. Added day 5.

## Design sketch

Two layers:

1. **Raw ingestion** — Kafka Engine tables that subscribe to analytics topics and deserialize Avro.
2. **Query layer** — `MergeTree` tables populated by `MATERIALIZED VIEW` from the Kafka tables. This is what Grafana queries.

## TODO (day 5)

- [ ] `init.sql` — create database `tsp`, Kafka engine tables, MergeTree tables, materialized views.
- [ ] One MergeTree per analytics topic, partitioned by day, ordered by `(symbol, event_time)`.
- [ ] Confirm with `SELECT count() FROM tsp.ohlcv_1s` after the pipeline runs for a minute.
