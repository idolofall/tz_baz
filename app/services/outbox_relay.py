import asyncio
import logging

from app.core.config import get_settings
from app.core.db import async_session_factory
from app.messaging.broker import broker, payments_exchange
from app.repositories import outbox as outbox_repo

logger = logging.getLogger(__name__)


async def publish_pending_outbox() -> int:
    """Publish pending outbox rows to RabbitMQ. Returns number of published events."""
    settings = get_settings()
    published = 0
    async with async_session_factory() as session:
        rows = await outbox_repo.lock_pending(session, limit=50)

        for row in rows:
            try:
                await broker.publish(
                    row.payload,
                    exchange=payments_exchange,
                    routing_key=settings.payments_routing_key,
                )
                outbox_repo.mark_published(row)
                published += 1
            except Exception:
                outbox_repo.record_publish_failure(row)
                logger.exception("Failed to publish outbox event %s", row.id)

        await session.commit()

    return published


async def outbox_relay_loop(stop_event: asyncio.Event) -> None:
    settings = get_settings()
    logger.info("Outbox relay started (interval=%ss)", settings.outbox_poll_interval_sec)

    while not stop_event.is_set():
        try:
            count = await publish_pending_outbox()
            if count:
                logger.info("Published %s outbox event(s)", count)
        except Exception:
            logger.exception("Outbox relay iteration failed")

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=settings.outbox_poll_interval_sec,
            )
        except TimeoutError:
            continue

    logger.info("Outbox relay stopped")
