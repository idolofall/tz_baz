from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import Outbox, OutboxStatus
from app.models.payment import Payment, PaymentStatus
from app.schemas.payment import PaymentCreate, PaymentCreatedResponse, PaymentResponse


def payment_to_response(payment: Payment) -> PaymentResponse:
    return PaymentResponse(
        id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_ or {},
        status=payment.status,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )


def payment_to_created_response(payment: Payment) -> PaymentCreatedResponse:
    return PaymentCreatedResponse(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


async def get_payment_by_id(session: AsyncSession, payment_id: UUID) -> Payment | None:
    result = await session.execute(select(Payment).where(Payment.id == payment_id))
    return result.scalar_one_or_none()


async def get_payment_by_idempotency_key(
    session: AsyncSession,
    idempotency_key: str,
) -> Payment | None:
    result = await session.execute(
        select(Payment).where(Payment.idempotency_key == idempotency_key)
    )
    return result.scalar_one_or_none()


async def create_payment(
    session: AsyncSession,
    payload: PaymentCreate,
    idempotency_key: str,
) -> tuple[Payment, bool]:
    """Create payment + outbox event. Returns (payment, created)."""
    existing = await get_payment_by_idempotency_key(session, idempotency_key)
    if existing is not None:
        return existing, False

    payment = Payment(
        amount=payload.amount,
        currency=payload.currency,
        description=payload.description,
        metadata_=payload.metadata,
        status=PaymentStatus.PENDING,
        idempotency_key=idempotency_key,
        webhook_url=str(payload.webhook_url),
    )
    session.add(payment)
    await session.flush()

    outbox = Outbox(
        aggregate_id=payment.id,
        event_type="payments.new",
        payload={
            "payment_id": str(payment.id),
            "amount": str(payment.amount),
            "currency": payment.currency.value,
            "webhook_url": payment.webhook_url,
        },
        status=OutboxStatus.PENDING,
        attempts=0,
    )
    session.add(outbox)
    await session.commit()
    await session.refresh(payment)
    return payment, True
