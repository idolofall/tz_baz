from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import get_settings

settings = get_settings()

broker = RabbitBroker(settings.rabbitmq_url)

payments_exchange = RabbitExchange(
    settings.payments_exchange,
    type=ExchangeType.DIRECT,
    durable=True,
)

payments_dlq = RabbitQueue(
    settings.payments_dlq,
    durable=True,
)

payments_queue = RabbitQueue(
    settings.payments_queue,
    durable=True,
    routing_key=settings.payments_routing_key,
    arguments={
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": settings.payments_dlq,
    },
)

payments_retry = RabbitQueue(
    settings.payments_retry_queue,
    durable=True,
    arguments={
        "x-dead-letter-exchange": settings.payments_exchange,
        "x-dead-letter-routing-key": settings.payments_routing_key,
    },
)


async def declare_topology() -> None:
    """Declare exchange, queues and bind payments.new to the exchange."""
    exchange = await broker.declare_exchange(payments_exchange)
    await broker.declare_queue(payments_dlq)
    await broker.declare_queue(payments_retry)
    queue = await broker.declare_queue(payments_queue)
    await queue.bind(exchange, routing_key=settings.payments_routing_key)
