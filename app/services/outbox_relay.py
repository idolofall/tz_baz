import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select

from app.core.config import get_settings
from app.core.db import async_session_factory
from app.messaging.broker import broker, payments_exchange
from app.models.outbox import Outbox, OutboxStatus

logger = logging.getLogger(__name__)


async def publish_pending_outbox() -> int:
    """Publish pending outbox rows to RabbitMQ. Returns number of published events."""
    settings = get_settings()
    published = 0
    async with async_session_factory() as session:
        result = await session.execute(
            select(Outbox)
            .where(Outbox.status == OutboxStatus.PENDING)
            .order_by(Outbox.created_at)
            .limit(50)
            .with_for_update(skip_locked=True)
        )
        rows = list(result.scalars().all())

        for row in rows:
            try:
                await broker.publish(
                    row.payload,
                    exchange=payments_exchange,
                    routing_key=settings.payments_routing_key,
                )
                row.status = OutboxStatus.PUBLISHED
                row.published_at = datetime.now(timezone.utc)
                row.attempts += 1
                published += 1
            except Exception:
                row.attempts += 1
                logger.exception("Failed to publish outbox event %s", row.id)
                if row.attempts >= 10:
                    row.status = OutboxStatus.FAILED

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
