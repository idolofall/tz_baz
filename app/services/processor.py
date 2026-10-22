import asyncio
import logging
import random
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select

from app.core.config import get_settings
from app.core.db import async_session_factory
from app.models.payment import Payment, PaymentStatus
from app.services.webhook import send_webhook

logger = logging.getLogger(__name__)


async def emulate_payment_gateway() -> PaymentStatus:
    settings = get_settings()
    delay = random.uniform(settings.payment_process_min_sec, settings.payment_process_max_sec)
    await asyncio.sleep(delay)
    if random.random() < settings.payment_success_rate:
        return PaymentStatus.SUCCEEDED
    return PaymentStatus.FAILED


async def process_payment_message(payment_id: UUID) -> None:
    """Emulate gateway, update DB status, notify webhook."""
    async with async_session_factory() as session:
        result = await session.execute(select(Payment).where(Payment.id == payment_id))
        payment = result.scalar_one_or_none()
        if payment is None:
            raise ValueError(f"Payment {payment_id} not found")

        if payment.status != PaymentStatus.PENDING:
            logger.info("Payment %s already processed with status %s", payment_id, payment.status)
            webhook_payload = {
                "payment_id": str(payment.id),
                "status": payment.status.value,
                "amount": str(payment.amount),
                "currency": payment.currency.value,
                "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
            }
            await send_webhook(payment.webhook_url, webhook_payload)
            return

        new_status = await emulate_payment_gateway()
        payment.status = new_status
        payment.processed_at = datetime.now(timezone.utc)
        await session.commit()
        await session.refresh(payment)

        webhook_payload = {
            "payment_id": str(payment.id),
            "status": payment.status.value,
            "amount": str(payment.amount),
            "currency": payment.currency.value,
            "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
        }
        await send_webhook(payment.webhook_url, webhook_payload)
        logger.info("Payment %s processed as %s", payment_id, new_status.value)
