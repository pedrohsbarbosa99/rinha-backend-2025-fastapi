import asyncio
import json

import httpx
import redis.asyncio as redis
from app.config import settings

redis_client = redis.Redis(
    host=settings.REDIS_HOST,
    decode_responses=False,
    max_connections=20,
)


def check_health(client, url: str):
    try:
        res = client.get(f"{url}/payments/service-health")
        if res.status_code == 429:
            return None
        res_json = res.json()
        return res_json.get("failing"), res_json.get("minResponseTime")
    except Exception:
        return None


async def set_url_checked():
    while True:
        with httpx.Client() as client:
            default_result = check_health(client, settings.PROCESSOR_DEFAULT_URL)
            if default_result:
                failing_default, res_time_default = default_result
                if not failing_default and res_time_default < 630:
                    await redis_client.set(
                        "checked",
                        json.dumps(
                            {
                                "url": settings.PROCESSOR_DEFAULT_URL,
                                "processor": "default",
                                "fail": failing_default,
                            },
                        ),
                        ex=10,
                    )
                    await asyncio.sleep(5)
                    continue

                fallback_result = check_health(client, settings.PROCESSOR_FALLBACK_URL)

                if fallback_result:
                    failing_fallback, res_time_fallback = fallback_result
                    if failing_fallback or (
                        not failing_default
                        and res_time_default > res_time_fallback * 1.3
                    ):
                        await redis_client.set(
                            "checked",
                            json.dumps(
                                {
                                    "url": settings.PROCESSOR_DEFAULT_URL,
                                    "processor": "default",
                                    "fail": failing_default,
                                }
                            ),
                            ex=10,
                        )
                        await asyncio.sleep(5)
                        continue

                    if not failing_fallback and res_time_fallback < 90:
                        await redis_client.set(
                            "checked",
                            json.dumps(
                                {
                                    "url": settings.PROCESSOR_FALLBACK_URL,
                                    "processor": "fallback",
                                    "fail": failing_fallback,
                                }
                            ),
                            ex=10,
                        )
                    await asyncio.sleep(5)
                    continue


if __name__ == "__main__":

    async def main():
        await redis_client.set(
            "checked",
            json.dumps(
                {
                    "url": settings.PROCESSOR_DEFAULT_URL,
                    "processor": "default",
                    "fail": False,
                }
            ),
            ex=10,
        )
        await set_url_checked()

    asyncio.run(main())
