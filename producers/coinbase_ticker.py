"""Coinbase Advanced Trade ticker → Redpanda producer (Avro).

Subscribes to the `ticker` channel for SOL-USD and SOL-USDT.
Each update is a best-bid/ask snapshot plus 24h stats.
Topics: coinbase.sol_usd.ticker, coinbase.sol_usdt.ticker
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

import structlog
import websockets
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

log = structlog.get_logger()

PRODUCTS = ["SOL-USD", "SOL-USDT"]
WS_URL = os.getenv("COINBASE_WS_URL", "wss://advanced-trade-ws.coinbase.com")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:18081")

_SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "ticker_event.avsc"


def _topic(product_id: str) -> str:
    return f"coinbase.{product_id.lower().replace('-', '_')}.ticker"


def _make_producer() -> SerializingProducer:
    schema_str = _SCHEMA_PATH.read_text()
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(sr_client, schema_str)
    return SerializingProducer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "linger.ms": 5,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
        }
    )


def _float_or_none(v: str | None) -> float | None:
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def _to_record(ticker: dict) -> dict:
    return {
        "product_id": ticker["product_id"],
        "price": float(ticker["price"]),
        "best_bid": float(ticker["best_bid"]),
        "best_ask": float(ticker["best_ask"]),
        "best_bid_quantity": float(ticker["best_bid_quantity"]),
        "best_ask_quantity": float(ticker["best_ask_quantity"]),
        "volume_24_h": _float_or_none(ticker.get("volume_24_h")),
        "low_24_h": _float_or_none(ticker.get("low_24_h")),
        "high_24_h": _float_or_none(ticker.get("high_24_h")),
        "price_percent_chg_24_h": _float_or_none(ticker.get("price_percent_chg_24_h")),
        "received_at": datetime.now(timezone.utc).isoformat(),
    }


def _delivery(err, msg) -> None:
    if err is not None:
        log.error("delivery_failed", error=str(err), topic=msg.topic())


async def run() -> None:
    producer = _make_producer()
    log.info("producer_starting", ws=WS_URL, products=PRODUCTS, bootstrap=BOOTSTRAP)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)

    subscribe = json.dumps(
        {"type": "subscribe", "product_ids": PRODUCTS, "channel": "ticker"}
    )

    async for ws in websockets.connect(WS_URL, ping_interval=20, ping_timeout=20):
        count = 0
        try:
            await ws.send(subscribe)
            log.info("ws_subscribed", channel="ticker")
            async for raw in ws:
                if stop.is_set():
                    break
                msg = json.loads(raw)
                if msg.get("channel") != "ticker":
                    continue
                for event in msg.get("events", []):
                    for ticker in event.get("tickers", []):
                        topic = _topic(ticker["product_id"])
                        producer.produce(
                            topic,
                            key=ticker["product_id"],
                            value=_to_record(ticker),
                            on_delivery=_delivery,
                        )
                        count += 1
                        if count % 50 == 0:
                            log.info("produced", count=count, product=ticker["product_id"], price=ticker["price"])
                producer.poll(0)
            if stop.is_set():
                break
        except websockets.ConnectionClosed as e:
            log.warning("ws_closed_reconnecting", code=e.code, reason=e.reason)
            continue

    producer.flush(5)
    log.info("producer_stopped")


if __name__ == "__main__":
    structlog.configure(processors=[structlog.processors.KeyValueRenderer()])
    asyncio.run(run())
