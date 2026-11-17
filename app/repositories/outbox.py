from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import Outbox, OutboxStatus


def add(session: AsyncSession, outbox: Outbox) -> Outbox:
    session.add(outbox)
    return outbox


async def lock_pending(session: AsyncSession, limit: int = 50) -> list[Outbox]:
    result = await session.execute(
        select(Outbox)
        .where(Outbox.status == OutboxStatus.PENDING)
        .order_by(Outbox.created_at)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    return list(result.scalars().all())


def mark_published(row: Outbox) -> None:
    row.status = OutboxStatus.PUBLISHED
    row.published_at = datetime.now(timezone.utc)
    row.attempts += 1


def record_publish_failure(row: Outbox, max_attempts: int = 10) -> None:
    row.attempts += 1
    if row.attempts >= max_attempts:
        row.status = OutboxStatus.FAILED
