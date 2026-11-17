import asyncio
import logging
import random
from uuid import UUID

from app.core.config import get_settings
from app.core.db import async_session_factory
from app.models.payment import Payment, PaymentStatus
from app.repositories import payment as payment_repo
from app.services.webhook import send_webhook

logger = logging.getLogger(__name__)


class PaymentLockedError(Exception):
    """Payment is claimed by another worker; retry the message later."""


def _webhook_payload(payment: Payment) -> dict:
    return {
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "amount": str(payment.amount),
        "currency": payment.currency.value,
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }


async def emulate_payment_gateway() -> PaymentStatus:
    settings = get_settings()
    delay = random.uniform(settings.payment_process_min_sec, settings.payment_process_max_sec)
    await asyncio.sleep(delay)
    if random.random() < settings.payment_success_rate:
        return PaymentStatus.SUCCEEDED
    return PaymentStatus.FAILED


async def _deliver_webhook(payment: Payment) -> None:
    await send_webhook(payment.webhook_url, _webhook_payload(payment))
    async with async_session_factory() as session:
        await payment_repo.mark_webhook_delivered(session, payment.id)
        await session.commit()


async def process_payment_message(payment_id: UUID) -> None:
    """Claim payment, emulate gateway outside the DB session, then notify webhook."""
    settings = get_settings()
    stale_after_sec = settings.payment_process_max_sec * 2

    async with async_session_factory() as session:
        claimed = await payment_repo.try_claim(session, payment_id, stale_after_sec)
        if claimed is not None:
            await session.commit()
        else:
            payment = await payment_repo.get_by_id(session, payment_id)
            if payment is None:
                raise ValueError(f"Payment {payment_id} not found")

            if payment.status == PaymentStatus.PROCESSING:
                raise PaymentLockedError(f"Payment {payment_id} is already being processed")

            if payment.status in {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}:
                if payment.webhook_delivered_at is not None:
                    logger.info(
                        "Payment %s already processed and webhook delivered",
                        payment_id,
                    )
                    return
                await session.commit()
                logger.info(
                    "Payment %s already processed with status %s, delivering webhook",
                    payment_id,
                    payment.status,
                )
                await _deliver_webhook(payment)
                return

            raise RuntimeError(
                f"Payment {payment_id} could not be claimed, status={payment.status}"
            )

    new_status = await emulate_payment_gateway()

    async with async_session_factory() as session:
        processed = await payment_repo.mark_processed(session, payment_id, new_status)
        if processed is None:
            raise RuntimeError(f"Payment {payment_id} was not in processing state")
        await session.commit()
        await session.refresh(processed)

    await _deliver_webhook(processed)
    logger.info("Payment %s processed as %s", payment_id, new_status.value)
