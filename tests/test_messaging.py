from faststream.rabbit import ExchangeType

from app.messaging.broker import (
    payments_dlq,
    payments_exchange,
    payments_queue,
)


def test_exchange_is_direct_durable():
    assert payments_exchange.name == "payments"
    assert payments_exchange.type == ExchangeType.DIRECT
    assert payments_exchange.durable is True


def test_queue_bound_routing_key():
    assert payments_queue.name == "payments.new"
    assert payments_queue.routing_key == "payments.new"
    assert payments_queue.durable is True


def test_dlq_configured():
    assert payments_dlq.name == "payments.new.dlq"
    assert payments_dlq.durable is True
    args = payments_queue.arguments or {}
    assert args.get("x-dead-letter-routing-key") == "payments.new.dlq"
