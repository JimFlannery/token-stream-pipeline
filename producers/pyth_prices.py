"""Pyth Hermes SSE → Redpanda producer (Avro).

Streams SOL/USD and USDC/USD price updates from Pyth Hermes.
Topics: pyth.sol_usd.prices, pyth.usdc_usd.prices

The USDC/USD feed also drives the Day 3 peg-drift Flink job.

Price representation: store raw integer mantissa + exponent.
Actual price = price_raw * 10^expo  (e.g. price_raw=8624000000, expo=-8 → $86.24)
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from pathlib import Path

import aiohttp
import structlog
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

log = structlog.get_logger()

HERMES_URL = os.getenv("PYTH_HERMES_URL", "https://hermes.pyth.network")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:18081")

# Feed IDs from .env.example (mainnet)
FEED_SOL_USD = os.getenv(
    "PYTH_FEED_SOL_USD",
    "ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d",
)
FEED_USDC_USD = os.getenv(
    "PYTH_FEED_USDC_USD",
    "eaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a",
)

# Map feed_id → Kafka topic (lowercase hex prefix is enough to identify)
FEED_TOPICS: dict[str, str] = {
    FEED_SOL_USD: "pyth.sol_usd.prices",
    FEED_USDC_USD: "pyth.usdc_usd.prices",
}

_SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "pyth_price.avsc"


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


def _to_record(parsed: dict) -> dict:
    p = parsed["price"]
    e = parsed["ema_price"]
    return {
        "feed_id": parsed["id"],
        "price_raw": int(p["price"]),
        "conf_raw": int(p["conf"]),
        "expo": int(p["expo"]),
        "publish_time": int(p["publish_time"]),
        "ema_price_raw": int(e["price"]),
        "ema_conf_raw": int(e["conf"]),
    }


def _delivery(err, msg) -> None:
    if err is not None:
        log.error("delivery_failed", error=str(err), topic=msg.topic())


async def run() -> None:
    producer = _make_producer()
    feed_ids = [FEED_SOL_USD, FEED_USDC_USD]
    params = "&".join(f"ids[]={fid}" for fid in feed_ids)
    stream_url = f"{HERMES_URL}/v2/updates/price/stream?{params}&parsed=true"
    log.info("producer_starting", url=stream_url, bootstrap=BOOTSTRAP)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)

    count = 0
    while not stop.is_set():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(stream_url, timeout=aiohttp.ClientTimeout(total=None)) as resp:
                    log.info("sse_connected", status=resp.status)
                    async for line in resp.content:
                        if stop.is_set():
                            break
                        text = line.decode().strip()
                        if not text.startswith("data:"):
                            continue
                        payload = json.loads(text[len("data:"):].strip())
                        for parsed in payload.get("parsed", []):
                            feed_id = parsed["id"]
                            topic = FEED_TOPICS.get(feed_id)
                            if topic is None:
                                continue
                            producer.produce(
                                topic,
                                key=feed_id,
                                value=_to_record(parsed),
                                on_delivery=_delivery,
                            )
                            count += 1
                            if count % 20 == 0:
                                expo = parsed["price"]["expo"]
                                actual = int(parsed["price"]["price"]) * (10 ** expo)
                                log.info("produced", count=count, topic=topic, price=f"{actual:.6f}")
                        producer.poll(0)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("sse_reconnecting", error=str(e))
            await asyncio.sleep(2)

    producer.flush(5)
    log.info("producer_stopped")


if __name__ == "__main__":
    structlog.configure(processors=[structlog.processors.KeyValueRenderer()])
    asyncio.run(run())
