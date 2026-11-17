import pytest
from faststream.rabbit import ExchangeType

from app.messaging.broker import (
    payments_dlq,
    payments_exchange,
    payments_queue,
    payments_retry,
)
from app.messaging.retry import retry_delay_ms


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


def test_retry_queue_dead_letters_to_main_exchange():
    assert payments_retry.name == "payments.retry"
    assert payments_retry.durable is True
    args = payments_retry.arguments or {}
    assert args.get("x-dead-letter-exchange") == "payments"
    assert args.get("x-dead-letter-routing-key") == "payments.new"


def test_retry_delay_exponential():
    assert retry_delay_ms(1, 1.0) == 1000
    assert retry_delay_ms(2, 1.0) == 2000
    assert retry_delay_ms(3, 1.0) == 4000
    assert retry_delay_ms(1, 0.5) == 500


def test_retry_delay_rejects_non_positive_attempt():
    with pytest.raises(ValueError, match="attempt must be >= 1"):
        retry_delay_ms(0, 1.0)
