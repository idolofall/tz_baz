import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import get_settings
from app.models.payment import PaymentStatus
from app.repositories import payment as payment_repo
from app.schemas.payment import PaymentCreate
from app.services.payment import create_payment
from app.services.processor import (
    PaymentLockedError,
    emulate_payment_gateway,
    process_payment_message,
)


@pytest.fixture
def processor_sessions(engine, monkeypatch):
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr("app.services.processor.async_session_factory", factory)
    return factory


async def _create_pending_payment(engine, key: str):
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        payload = PaymentCreate(
            amount=Decimal("10.00"),
            currency="USD",
            description="processor",
            metadata={},
            webhook_url="https://example.com/hook",
        )
        payment, created = await create_payment(session, payload, key)
        assert created is True
        return payment.id


@pytest.mark.asyncio
async def test_claim_prevents_duplicate_gateway(engine, clean_db, processor_sessions, monkeypatch):
    payment_id = await _create_pending_payment(engine, "claim-dup")
    calls = {"gateway": 0, "webhook": 0}

    async def fake_gateway():
        calls["gateway"] += 1
        await asyncio.sleep(0.2)
        return PaymentStatus.SUCCEEDED

    async def fake_webhook(_url, _payload):
        calls["webhook"] += 1

    monkeypatch.setattr("app.services.processor.emulate_payment_gateway", fake_gateway)
    monkeypatch.setattr("app.services.processor.send_webhook", fake_webhook)

    results = await asyncio.gather(
        process_payment_message(payment_id),
        process_payment_message(payment_id),
        return_exceptions=True,
    )

    errors = [item for item in results if isinstance(item, Exception)]
    assert None in results
    assert all(isinstance(item, PaymentLockedError) for item in errors)
    assert calls["gateway"] == 1
    assert calls["webhook"] == 1


@pytest.mark.asyncio
async def test_webhook_not_resent_after_delivery(engine, clean_db, processor_sessions, monkeypatch):
    payment_id = await _create_pending_payment(engine, "webhook-once")
    calls = {"webhook": 0}

    async def fake_gateway():
        return PaymentStatus.SUCCEEDED

    async def fake_webhook(_url, _payload):
        calls["webhook"] += 1

    monkeypatch.setattr("app.services.processor.emulate_payment_gateway", fake_gateway)
    monkeypatch.setattr("app.services.processor.send_webhook", fake_webhook)

    await process_payment_message(payment_id)
    await process_payment_message(payment_id)

    assert calls["webhook"] == 1


@pytest.mark.asyncio
async def test_webhook_resent_if_previous_delivery_failed(engine, clean_db, processor_sessions, monkeypatch):
    payment_id = await _create_pending_payment(engine, "webhook-retry")
    calls = {"webhook": 0, "gateway": 0}

    async def fake_gateway():
        calls["gateway"] += 1
        return PaymentStatus.FAILED

    async def fake_webhook(_url, _payload):
        calls["webhook"] += 1
        if calls["webhook"] == 1:
            raise RuntimeError("webhook down")

    monkeypatch.setattr("app.services.processor.emulate_payment_gateway", fake_gateway)
    monkeypatch.setattr("app.services.processor.send_webhook", fake_webhook)

    with pytest.raises(RuntimeError, match="webhook down"):
        await process_payment_message(payment_id)

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        payment = await payment_repo.get_by_id(session, payment_id)
        assert payment is not None
        assert payment.status == PaymentStatus.FAILED
        assert payment.webhook_delivered_at is None

    await process_payment_message(payment_id)
    assert calls["gateway"] == 1
    assert calls["webhook"] == 2

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        payment = await payment_repo.get_by_id(session, payment_id)
        assert payment is not None
        assert payment.status == PaymentStatus.FAILED
        assert payment.webhook_delivered_at is not None


@pytest.mark.asyncio
async def test_stale_processing_is_reclaimed(engine, clean_db, processor_sessions, monkeypatch):
    payment_id = await _create_pending_payment(engine, "stale-claim")
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        payment = await payment_repo.get_by_id(session, payment_id)
        assert payment is not None
        payment.status = PaymentStatus.PROCESSING
        payment.processing_started_at = datetime.now(timezone.utc) - timedelta(seconds=60)
        await session.commit()

    calls = {"gateway": 0}

    async def fake_gateway():
        calls["gateway"] += 1
        return PaymentStatus.SUCCEEDED

    async def fake_webhook(_url, _payload):
        return None

    monkeypatch.setattr("app.services.processor.emulate_payment_gateway", fake_gateway)
    monkeypatch.setattr("app.services.processor.send_webhook", fake_webhook)

    await process_payment_message(payment_id)
    assert calls["gateway"] == 1


@pytest.mark.asyncio
async def test_missing_payment_raises(engine, clean_db, processor_sessions):
    missing_id = uuid4()
    with pytest.raises(ValueError, match=str(missing_id)):
        await process_payment_message(missing_id)


@pytest.mark.asyncio
async def test_fresh_processing_is_locked(engine, clean_db, processor_sessions, monkeypatch):
    payment_id = await _create_pending_payment(engine, "fresh-lock")
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        payment = await payment_repo.get_by_id(session, payment_id)
        assert payment is not None
        payment.status = PaymentStatus.PROCESSING
        payment.processing_started_at = datetime.now(timezone.utc)
        await session.commit()

    calls = {"gateway": 0}

    async def fake_gateway():
        calls["gateway"] += 1
        return PaymentStatus.SUCCEEDED

    monkeypatch.setattr("app.services.processor.emulate_payment_gateway", fake_gateway)

    with pytest.raises(PaymentLockedError):
        await process_payment_message(payment_id)
    assert calls["gateway"] == 0


@pytest.mark.asyncio
async def test_processing_without_started_at_stays_locked(
    engine, clean_db, processor_sessions, monkeypatch
):
    payment_id = await _create_pending_payment(engine, "null-started")
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        payment = await payment_repo.get_by_id(session, payment_id)
        assert payment is not None
        payment.status = PaymentStatus.PROCESSING
        payment.processing_started_at = None
        await session.commit()

    async def fake_gateway():
        raise AssertionError("gateway must not run")

    monkeypatch.setattr("app.services.processor.emulate_payment_gateway", fake_gateway)

    with pytest.raises(PaymentLockedError):
        await process_payment_message(payment_id)


@pytest.mark.asyncio
async def test_unclaimed_pending_is_an_error(engine, clean_db, processor_sessions, monkeypatch):
    payment_id = await _create_pending_payment(engine, "unclaimed")

    async def no_claim(*_args, **_kwargs):
        return None

    monkeypatch.setattr("app.services.processor.payment_repo.try_claim", no_claim)

    with pytest.raises(RuntimeError, match="could not be claimed"):
        await process_payment_message(payment_id)


@pytest.mark.asyncio
async def test_gateway_crash_keeps_processing_until_stale_reclaim(
    engine, clean_db, processor_sessions, monkeypatch
):
    settings = get_settings()
    monkeypatch.setattr(settings, "payment_process_max_sec", 5)
    payment_id = await _create_pending_payment(engine, "gateway-crash")
    calls = {"gateway": 0, "webhook": 0}

    async def crash():
        calls["gateway"] += 1
        raise RuntimeError("gateway down")

    async def succeed():
        calls["gateway"] += 1
        return PaymentStatus.SUCCEEDED

    async def fake_webhook(url, payload):
        calls["webhook"] += 1
        calls["url"] = url
        calls["payload"] = payload

    monkeypatch.setattr("app.services.processor.emulate_payment_gateway", crash)
    monkeypatch.setattr("app.services.processor.send_webhook", fake_webhook)

    with pytest.raises(RuntimeError, match="gateway down"):
        await process_payment_message(payment_id)

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        payment = await payment_repo.get_by_id(session, payment_id)
        assert payment is not None
        assert payment.status == PaymentStatus.PROCESSING
        assert payment.processed_at is None

    with pytest.raises(PaymentLockedError):
        await process_payment_message(payment_id)
    assert calls["gateway"] == 1
    assert calls["webhook"] == 0

    async with session_factory() as session:
        payment = await payment_repo.get_by_id(session, payment_id)
        assert payment is not None
        payment.processing_started_at = datetime.now(timezone.utc) - timedelta(seconds=60)
        await session.commit()

    monkeypatch.setattr("app.services.processor.emulate_payment_gateway", succeed)
    await process_payment_message(payment_id)

    assert calls["gateway"] == 2
    assert calls["webhook"] == 1
    assert calls["url"] == "https://example.com/hook"
    assert calls["payload"]["payment_id"] == str(payment_id)
    assert calls["payload"]["status"] == "succeeded"
    assert calls["payload"]["amount"] == "10.00"
    assert calls["payload"]["currency"] == "USD"
    assert calls["payload"]["processed_at"]

    async with session_factory() as session:
        payment = await payment_repo.get_by_id(session, payment_id)
        assert payment is not None
        assert payment.status == PaymentStatus.SUCCEEDED
        assert payment.webhook_delivered_at is not None


@pytest.mark.asyncio
async def test_lost_processing_state_does_not_send_webhook(
    engine, clean_db, processor_sessions, monkeypatch
):
    payment_id = await _create_pending_payment(engine, "lost-processing")
    calls = {"webhook": 0}

    async def fake_gateway():
        return PaymentStatus.SUCCEEDED

    async def fake_mark(*_args, **_kwargs):
        return None

    async def fake_webhook(_url, _payload):
        calls["webhook"] += 1

    monkeypatch.setattr("app.services.processor.emulate_payment_gateway", fake_gateway)
    monkeypatch.setattr("app.services.processor.payment_repo.mark_processed", fake_mark)
    monkeypatch.setattr("app.services.processor.send_webhook", fake_webhook)

    with pytest.raises(RuntimeError, match="not in processing state"):
        await process_payment_message(payment_id)
    assert calls["webhook"] == 0


@pytest.mark.asyncio
async def test_gateway_emulation_follows_success_rate(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "payment_process_min_sec", 0)
    monkeypatch.setattr(settings, "payment_process_max_sec", 0)
    monkeypatch.setattr(settings, "payment_success_rate", 0.5)
    monkeypatch.setattr("app.services.processor.random.random", lambda: 0.49)
    assert await emulate_payment_gateway() == PaymentStatus.SUCCEEDED

    monkeypatch.setattr("app.services.processor.random.random", lambda: 0.5)
    assert await emulate_payment_gateway() == PaymentStatus.FAILED
