import asyncio
import json
from datetime import datetime, timezone

import httpx
import redis.asyncio as redis
from app.config import settings

redis_client = redis.Redis(host=settings.REDIS_HOST, decode_responses=False)

CACHED_DATA = None
CACHE_TIMESTAMP = None
CACHE_TTL = 60

queue = asyncio.Queue(maxsize=50000)


async def get_config_data():
    global CACHED_DATA, CACHE_TIMESTAMP

    now = datetime.now()

    if CACHED_DATA and CACHE_TIMESTAMP and (now - CACHE_TIMESTAMP).seconds < CACHE_TTL:
        return CACHED_DATA

    data = {}
    raw = await redis_client.get("checked")
    if raw:
        data = json.loads(raw)

    if not data:
        CACHED_DATA = {"url": settings.PROCESSOR_DEFAULT_URL, "processor": "default"}
    else:
        CACHED_DATA = data

    CACHE_TIMESTAMP = now
    return CACHED_DATA


async def add_to_queue(correlation_id, amount):
    try:
        await queue.put({"correlationId": correlation_id, "amount": amount})
    except asyncio.QueueFull:
        await process_payment_immediate(
            {"correlationId": correlation_id, "amount": amount}
        )


async def process_payment_immediate(payload):
    processor, requested_at, ok = await post_payment(payload)

    if ok:
        timestamp = requested_at.timestamp()
        entry = json.dumps(
            {
                "cid": payload["correlationId"],
                "amount": payload["amount"],
                "processor": processor,
                "requested_at": timestamp,
            }
        )
        await redis_client.zadd("processed", {entry: timestamp})
    else:
        await redis_client.rpush("payments_failure", json.dumps(payload))


http_client = None


async def get_http_client():
    global http_client
    if http_client is None:
        timeout = httpx.Timeout(3.0, connect=1.0, read=2.0)
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=100)
        http_client = httpx.AsyncClient(timeout=timeout, limits=limits)
    return http_client


async def post_payment(payload, retry_count=0):
    requested_at = datetime.now(tz=timezone.utc)
    processor = ""
    data = await get_config_data()

    if not data.get("url"):
        return processor, requested_at, False

    client = await get_http_client()

    try:
        res = await client.post(
            f"{data['url']}/payments",
            json=payload,
        )
        if 200 <= res.status_code < 300:
            return data["processor"], requested_at, True
    except Exception:
        if retry_count < 1:
            await asyncio.sleep(0.05)
            return await post_payment(payload, retry_count + 1)
        pass
    return processor, requested_at, False


async def worker():
    batch_size = 35
    batch = []

    while True:
        try:
            while len(batch) < batch_size:
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=0.05)
                    batch.append(payload)
                except asyncio.TimeoutError:
                    break

            if not batch:
                continue

            tasks = []
            for payload in batch:
                task = asyncio.create_task(process_payment_with_redis(payload))
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    await process_payment_with_redis(batch[i])

            batch.clear()

        except Exception as e:
            print(f"Worker error: {e}")
            continue


async def process_payment_with_redis(payload):
    processor, requested_at, ok = await post_payment(payload)

    if ok:
        timestamp = requested_at.timestamp()
        entry = json.dumps(
            {
                "cid": payload["correlationId"],
                "amount": payload["amount"],
                "processor": processor,
                "requested_at": timestamp,
            }
        )
        await redis_client.zadd("processed", {entry: timestamp})
    else:
        await redis_client.rpush("payments_failure", json.dumps(payload))


async def worker_main():
    worker_count = 10
    await asyncio.gather(*[worker() for _ in range(worker_count)])
