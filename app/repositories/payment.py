from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment import Payment, PaymentStatus


async def get_by_id(session: AsyncSession, payment_id: UUID) -> Payment | None:
    result = await session.execute(select(Payment).where(Payment.id == payment_id))
    return result.scalar_one_or_none()


async def get_by_idempotency_key(
    session: AsyncSession,
    idempotency_key: str,
) -> Payment | None:
    result = await session.execute(
        select(Payment).where(Payment.idempotency_key == idempotency_key)
    )
    return result.scalar_one_or_none()


def add(session: AsyncSession, payment: Payment) -> Payment:
    session.add(payment)
    return payment


async def try_claim(
    session: AsyncSession,
    payment_id: UUID,
    stale_after_sec: float,
) -> Payment | None:
    """Atomically capture a pending or stale-processing payment."""
    stale_before = datetime.now(timezone.utc) - timedelta(seconds=stale_after_sec)
    stmt = (
        update(Payment)
        .where(
            Payment.id == payment_id,
            or_(
                Payment.status == PaymentStatus.PENDING,
                and_(
                    Payment.status == PaymentStatus.PROCESSING,
                    Payment.processing_started_at.is_not(None),
                    Payment.processing_started_at <= stale_before,
                ),
            ),
        )
        .values(
            status=PaymentStatus.PROCESSING,
            processing_started_at=datetime.now(timezone.utc),
        )
        .returning(Payment)
        .execution_options(synchronize_session="fetch")
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def mark_processed(
    session: AsyncSession,
    payment_id: UUID,
    status: PaymentStatus,
) -> Payment | None:
    stmt = (
        update(Payment)
        .where(
            Payment.id == payment_id,
            Payment.status == PaymentStatus.PROCESSING,
        )
        .values(
            status=status,
            processed_at=datetime.now(timezone.utc),
        )
        .returning(Payment)
        .execution_options(synchronize_session="fetch")
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def mark_webhook_delivered(
    session: AsyncSession,
    payment_id: UUID,
) -> None:
    await session.execute(
        update(Payment)
        .where(
            Payment.id == payment_id,
            Payment.webhook_delivered_at.is_(None),
        )
        .values(webhook_delivered_at=datetime.now(timezone.utc))
    )
