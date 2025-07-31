import asyncio
from datetime import datetime, timezone

import httpx
import orjson
import redis.asyncio as redis
from app.config import settings

redis_client = redis.Redis(
    host=settings.REDIS_HOST,
    decode_responses=False,
    max_connections=100,
)

CACHE_DATA = {}
CACHE_TIMESTAMP = None
CACHE_TTL = 4.8
queue = asyncio.Queue(maxsize=50000)
semaphore = asyncio.Semaphore(20)


async def get_config_data():
    return {
        "url": settings.PROCESSOR_DEFAULT_URL,
        "processor": "default",
        "fail": False,
    }


async def add_to_queue(correlation_id, amount):
    await queue.put({"correlationId": correlation_id, "amount": amount})


http_client = None


async def get_http_client():
    global http_client
    if http_client is None:
        timeout = httpx.Timeout(3.0, connect=1.0, read=2.0)
        limits = httpx.Limits(max_keepalive_connections=10, max_connections=100)
        http_client = httpx.AsyncClient(timeout=timeout, limits=limits)
    return http_client


async def post_payment(payload, url, processor, requested_at):
    client = await get_http_client()

    iso_format = requested_at.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    payload["requestedAt"] = iso_format

    try:
        res = await client.post(
            f"{url}/payments",
            json=payload,
        )
        if 200 <= res.status_code < 300:
            return processor, True
        else:
            await queue.put(payload)
    except Exception:
        pass
    return processor, False


async def _process_payment(payload, url, processor):
    async with semaphore:
        return await process_payment(payload, url, processor)


async def worker():
    while True:
        data = await get_config_data()
        concurrency = 30

        tasks = []
        for _ in range(concurrency):
            payload = await queue.get()
            tasks.append(
                _process_payment(
                    payload,
                    data["url"],
                    data["processor"],
                )
            )
        await asyncio.gather(*tasks)


async def process_payment(payload, url, processor):
    requested_at = datetime.now(tz=timezone.utc)
    timestamp = requested_at.timestamp()
    entry = orjson.dumps(
        {
            "cid": payload["correlationId"][:9],
            "amount": payload["amount"],
            "processor": processor,
            "requested_at": timestamp,
        }
    )

    processor, ok = await post_payment(payload, url, processor, requested_at)

    if ok:
        await redis_client.zadd("processed", {entry: timestamp})
    else:
        await add_to_queue(payload["correlationId"], payload["amount"])
    return ok


async def worker_main():
    worker_count = 1
    await asyncio.gather(*[worker() for _ in range(worker_count)])
