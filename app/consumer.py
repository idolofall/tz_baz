import logging
from uuid import UUID

from faststream import FastStream
from faststream.rabbit import RabbitMessage

from app.core.config import get_settings
from app.messaging.broker import (
    broker,
    payments_dlq,
    payments_exchange,
    payments_queue,
)
from app.services.processor import process_payment_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = FastStream(broker)
settings = get_settings()


@broker.subscriber(payments_dlq)
async def on_dead_letter(body: dict) -> None:
    logger.error("Dead letter received: %s", body)


@broker.subscriber(payments_queue, payments_exchange)
async def on_payment_new(body: dict, message: RabbitMessage) -> None:
    headers = dict(message.headers or {})
    retry_count = int(headers.get("x-retry-count", 0))
    payment_id_raw = body.get("payment_id")

    try:
        if not payment_id_raw:
            raise ValueError("payment_id missing in message payload")
        payment_id = UUID(str(payment_id_raw))
        await process_payment_message(payment_id)
        logger.info("Successfully processed payment %s", payment_id)
    except Exception as exc:
        next_retry = retry_count + 1
        logger.exception(
            "Payment processing failed (attempt %s/%s): %s",
            next_retry,
            settings.consumer_max_retries,
            exc,
        )
        if next_retry >= settings.consumer_max_retries:
            await broker.publish(
                {
                    **body,
                    "error": str(exc),
                    "attempts": next_retry,
                },
                queue=payments_dlq,
            )
            logger.error("Payment %s moved to DLQ after %s attempts", payment_id_raw, next_retry)
            return

        await broker.publish(
            body,
            exchange=payments_exchange,
            routing_key=settings.payments_routing_key,
            headers={**headers, "x-retry-count": next_retry},
        )
        logger.warning("Payment %s requeued (attempt %s)", payment_id_raw, next_retry)
