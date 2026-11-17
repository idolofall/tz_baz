from decimal import Decimal
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import Outbox, OutboxStatus
from app.models.payment import Payment, PaymentStatus
from app.repositories import outbox as outbox_repo
from app.repositories import payment as payment_repo
from app.schemas.payment import PaymentCreate, PaymentCreatedResponse, PaymentResponse

_AMOUNT_QUANT = Decimal("0.01")


class IdempotencyConflict(Exception):
    """Same Idempotency-Key was already used with a different request body."""


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


def payloads_match(payment: Payment, payload: PaymentCreate) -> bool:
    return (
        payment.amount.quantize(_AMOUNT_QUANT) == payload.amount.quantize(_AMOUNT_QUANT)
        and payment.currency == payload.currency
        and payment.description == payload.description
        and (payment.metadata_ or {}) == (payload.metadata or {})
        and payment.webhook_url == str(payload.webhook_url)
    )


def _existing_or_conflict(existing: Payment, payload: PaymentCreate) -> Payment:
    if not payloads_match(existing, payload):
        raise IdempotencyConflict
    return existing


def _is_idempotency_key_conflict(exc: IntegrityError) -> bool:
    message = str(exc.orig).lower() if exc.orig is not None else str(exc).lower()
    return "idempotency_key" in message or "ix_payments_idempotency_key" in message


async def get_payment_by_id(session: AsyncSession, payment_id: UUID) -> Payment | None:
    return await payment_repo.get_by_id(session, payment_id)


async def create_payment(
    session: AsyncSession,
    payload: PaymentCreate,
    idempotency_key: str,
) -> tuple[Payment, bool]:
    """Create payment + outbox event. Returns (payment, created)."""
    existing = await payment_repo.get_by_idempotency_key(session, idempotency_key)
    if existing is not None:
        return _existing_or_conflict(existing, payload), False

    payment = Payment(
        amount=payload.amount,
        currency=payload.currency,
        description=payload.description,
        metadata_=payload.metadata,
        status=PaymentStatus.PENDING,
        idempotency_key=idempotency_key,
        webhook_url=str(payload.webhook_url),
    )
    payment_repo.add(session, payment)

    try:
        await session.flush()
    except IntegrityError as exc:
        if not _is_idempotency_key_conflict(exc):
            raise
        await session.rollback()
        raced = await payment_repo.get_by_idempotency_key(session, idempotency_key)
        if raced is None:
            raise
        return _existing_or_conflict(raced, payload), False

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
    outbox_repo.add(session, outbox)
    await session.commit()
    await session.refresh(payment)
    return payment, True
