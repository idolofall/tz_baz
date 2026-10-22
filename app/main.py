import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.payments import router as payments_router
from app.messaging.broker import broker, declare_topology
from app.services.outbox_relay import outbox_relay_loop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    stop_event = asyncio.Event()
    await broker.start()
    await declare_topology()
    relay_task = asyncio.create_task(outbox_relay_loop(stop_event))
    logger.info("API started with outbox relay")
    try:
        yield
    finally:
        stop_event.set()
        await relay_task
        await broker.close()
        logger.info("API stopped")


app = FastAPI(
    title="Payment Processing Service",
    version="1.0.0",
    lifespan=lifespan,
)
app.include_router(payments_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
