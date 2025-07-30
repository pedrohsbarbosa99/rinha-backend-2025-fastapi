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
semaphore = asyncio.Semaphore(10)


async def get_config_data():
    global CACHE_DATA, CACHE_TIMESTAMP
    if CACHE_DATA and (
        CACHE_TIMESTAMP
        and (datetime.now() - CACHE_TIMESTAMP).total_seconds() < CACHE_TTL
    ):
        return CACHE_DATA
    raw = await redis_client.get("checked")
    if raw:
        CACHE_DATA = orjson.loads(raw)
        CACHE_TIMESTAMP = datetime.now()

    return CACHE_DATA


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
    except Exception as e:
        print(f"DEU ERRO: {e}", flush=True)
    return processor, False


async def _process_payment(payload, url, processor):
    async with semaphore:
        return await process_payment(payload, url, processor)


async def worker():
    while True:
        data = await get_config_data()
        concurrency = 40
        if data.get("fail", False):
            await asyncio.sleep(2.5)
            data = await get_config_data()
            if not data or data.get("fail", False):
                await asyncio.sleep(0.5)
            concurrency = 1

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
        results = await asyncio.gather(*tasks)
        queue.task_done()
        if not all(results):
            data = await get_config_data()
            if data and data.get("fail", False):
                await asyncio.sleep(1)


async def process_payment(payload, url, processor):
    requested_at = datetime.now(tz=timezone.utc)
    timestamp = requested_at.timestamp()
    entry = orjson.dumps(
        {
            "cid": payload["correlationId"],
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
