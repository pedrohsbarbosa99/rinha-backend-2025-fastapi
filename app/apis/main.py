import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import redis.asyncio as redis
from app.config import settings
from app.worker import add_to_queue, worker_main
from fastapi import BackgroundTasks, FastAPI, Query
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel

redis_client = redis.Redis(host=settings.REDIS_HOST, decode_responses=True)


@asynccontextmanager
async def lifespan(_: FastAPI):
    worker_task = asyncio.create_task(worker_main())
    yield
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan, default_response_class=ORJSONResponse)


class PaymentRequest(BaseModel):
    correlationId: str
    amount: float


@app.post("/payments")
async def create_payment(payment: PaymentRequest, bg: BackgroundTasks):
    bg.add_task(add_to_queue, payment.correlationId, payment.amount)


@app.get("/payments-summary")
async def payments_summary(
    from_: datetime = Query(None, alias="from"),
    to: datetime = Query(None, alias="to"),
):
    from_ts = from_.astimezone(timezone.utc).timestamp() if from_ else "-inf"
    to_ts = to.astimezone(timezone.utc).timestamp() if from_ else "+inf"

    results = await redis_client.zrangebyscore("processed", min=from_ts, max=to_ts)

    data = {
        "default": {"totalRequests": 0, "totalAmount": 0.0},
        "fallback": {"totalRequests": 0, "totalAmount": 0.0},
    }

    for item in results:
        result = json.loads(item)
        processor = result.get("processor")
        amount = float(result.get("amount", 0))

        if processor in data:
            data[processor]["totalRequests"] += 1
            data[processor]["totalAmount"] = round(
                data[processor]["totalAmount"] + amount,
                2,
            )

    return data
