"""Coinbase Advanced Trade market_trades → Redpanda producer (Avro, Day 2+).

Binance.com is geoblocked in the US; Binance.US silently disabled @trade/@aggTrade WS
streams. Coinbase Advanced Trade is the US-legal real-time trade source.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from pathlib import Path

import structlog
import websockets
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

log = structlog.get_logger()

PRODUCT_ID = "SOL-USD"  # Coinbase doesn't list SOL-USDC — USD book is USDC-backed.
TOPIC = f"coinbase.{PRODUCT_ID.lower().replace('-', '_')}.trades"
WS_URL = os.getenv("COINBASE_WS_URL", "wss://advanced-trade-ws.coinbase.com")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:18081")

_SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "trade_event.avsc"


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


def _to_record(trade: dict) -> dict:
    side = trade.get("side", "UNKNOWN")
    if side not in ("BUY", "SELL"):
        side = "UNKNOWN"
    return {
        "product_id": trade["product_id"],
        "trade_id": trade["trade_id"],
        "price": float(trade["price"]),
        "size": float(trade["size"]),
        "time": trade["time"],
        "side": side,
    }


def _delivery(err, msg) -> None:
    if err is not None:
        log.error("delivery_failed", error=str(err), topic=msg.topic())


async def run() -> None:
    producer = _make_producer()
    log.info("producer_starting", ws=WS_URL, topic=TOPIC, product=PRODUCT_ID, bootstrap=BOOTSTRAP)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)

    subscribe = json.dumps(
        {"type": "subscribe", "product_ids": [PRODUCT_ID], "channel": "market_trades"}
    )

    async for ws in websockets.connect(WS_URL, ping_interval=20, ping_timeout=20):
        count = 0
        try:
            await ws.send(subscribe)
            log.info("ws_subscribed", channel="market_trades")
            async for raw in ws:
                if stop.is_set():
                    break
                msg = json.loads(raw)
                if msg.get("channel") != "market_trades":
                    continue
                for event in msg.get("events", []):
                    # Skip the initial snapshot — it's historical replay, not live ticks.
                    if event.get("type") == "snapshot":
                        continue
                    for trade in event.get("trades", []):
                        producer.produce(
                            TOPIC,
                            key=trade["product_id"],
                            value=_to_record(trade),
                            on_delivery=_delivery,
                        )
                        count += 1
                        if count % 25 == 0:
                            log.info("produced", count=count, price=trade["price"])
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
