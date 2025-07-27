import asyncio
from datetime import datetime, timezone

import httpx
import orjson
import redis.asyncio as redis
from app.config import settings

redis_client = redis.Redis(host=settings.REDIS_HOST, decode_responses=False)

CACHED_DATA = None
CACHE_TIMESTAMP = None
CACHE_TTL = 5

queue = asyncio.Queue(maxsize=50000)


async def get_config_data():
    global CACHED_DATA, CACHE_TIMESTAMP

    now = datetime.now()

    if CACHED_DATA and CACHE_TIMESTAMP and (now - CACHE_TIMESTAMP).seconds < CACHE_TTL:
        return CACHED_DATA

    raw = await redis_client.get("checked")
    if raw:
        CACHED_DATA = orjson.loads(raw)

    CACHE_TIMESTAMP = now
    return CACHED_DATA or {}


async def add_to_queue(correlation_id, amount):
    await queue.put({"correlationId": correlation_id, "amount": amount})


async def process_payment_immediate(payload):
    processor, requested_at, ok = await post_payment(payload)

    if ok:
        timestamp = requested_at.timestamp()
        entry = orjson.dumps(
            {
                "cid": payload["correlationId"],
                "amount": payload["amount"],
                "processor": processor,
                "requested_at": timestamp,
            }
        )
        await redis_client.zadd("processed", {entry: timestamp})
    else:
        await redis_client.rpush("payments_failure", orjson.dumps(payload))


http_client = None


async def get_http_client():
    global http_client
    if http_client is None:
        timeout = httpx.Timeout(3.0, connect=1.0, read=2.0)
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=100)
        http_client = httpx.AsyncClient(timeout=timeout, limits=limits)
    return http_client


async def post_payment(payload):
    processor = ""
    data = await get_config_data()

    client = await get_http_client()

    requested_at = datetime.now(tz=timezone.utc)
    iso_format = requested_at.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    payload["requestedAt"] = iso_format

    try:
        res = await client.post(
            f"{data['url']}/payments",
            json=payload,
        )
        if 200 <= res.status_code < 300:
            return data["processor"], requested_at, True
    except Exception:
        pass
    return processor, requested_at, False


async def worker():
    while True:
        payload = await queue.get()
        await process_payment(payload)


async def process_payment(payload):
    processor, requested_at, ok = await post_payment(payload)

    if ok:
        timestamp = requested_at.timestamp()
        entry = orjson.dumps(
            {
                "cid": payload["correlationId"],
                "amount": payload["amount"],
                "processor": processor,
                "requested_at": timestamp,
            }
        )
        await redis_client.zadd("processed", {entry: timestamp})
    else:
        await add_to_queue(payload["correlationId"], payload["amount"])


async def worker_main():
    worker_count = 1
    await asyncio.gather(*[worker() for _ in range(worker_count)])
