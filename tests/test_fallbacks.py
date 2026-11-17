import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer import on_payment_new
from app.core.config import get_settings
from app.messaging.broker import payments_dlq, payments_exchange, payments_retry
from app.models.outbox import Outbox, OutboxStatus
from app.repositories import outbox as outbox_repo
from app.services.outbox_relay import outbox_relay_loop, publish_pending_outbox
from app.services.processor import PaymentLockedError


class _Message:
    def __init__(self, headers=None):
        self.headers = headers


def _use_consumer_settings(monkeypatch, **overrides):
    settings = get_settings().model_copy(
        update={
            "consumer_max_retries": 3,
            "consumer_retry_base_delay_sec": 1.0,
            **overrides,
        }
    )
    monkeypatch.setattr("app.consumer.settings", settings)
    return settings


def _capture_publish(monkeypatch, target: str):
    published: list[tuple] = []

    async def fake_publish(body, **kwargs):
        published.append((body, kwargs))

    monkeypatch.setattr(target, fake_publish)
    return published


def _stub_process(monkeypatch, error: Exception | None = None):
    seen: list = []

    async def process(payment_id):
        seen.append(payment_id)
        if error is not None:
            raise error

    monkeypatch.setattr("app.consumer.process_payment_message", process)
    return seen


def _outbox(marker: str, *, created_at=None, attempts: int = 0, status=OutboxStatus.PENDING):
    row = Outbox(
        aggregate_id=uuid4(),
        event_type="payments.new",
        payload={"marker": marker},
        status=status,
        attempts=attempts,
    )
    if created_at is not None:
        row.created_at = created_at
    return row


async def _insert_outbox(engine, *rows: Outbox) -> None:
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        session.add_all(rows)
        await session.commit()


@pytest.fixture
def relay_sessions(engine, monkeypatch):
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr("app.services.outbox_relay.async_session_factory", factory)
    return factory


@pytest.mark.asyncio
async def test_consumer_success_does_not_republish(monkeypatch):
    _use_consumer_settings(monkeypatch)
    published = _capture_publish(monkeypatch, "app.consumer.broker.publish")
    seen = _stub_process(monkeypatch)
    payment_id = uuid4()

    await on_payment_new({"payment_id": str(payment_id)}, _Message())

    assert seen == [payment_id]
    assert published == []


@pytest.mark.asyncio
async def test_consumer_retries_with_backoff_and_keeps_headers(monkeypatch):
    _use_consumer_settings(monkeypatch, consumer_retry_base_delay_sec=0.5, consumer_max_retries=5)
    published = _capture_publish(monkeypatch, "app.consumer.broker.publish")
    _stub_process(monkeypatch, PaymentLockedError("busy"))
    body = {"payment_id": str(uuid4())}

    await on_payment_new(body, _Message(headers={"x-retry-count": "1", "x-trace": "abc"}))

    assert len(published) == 1
    retried, kwargs = published[0]
    assert retried is body
    assert "error" not in body
    assert kwargs["queue"] is payments_retry
    assert kwargs["expiration"] == 1000
    assert kwargs["headers"]["x-retry-count"] == 2
    assert kwargs["headers"]["x-trace"] == "abc"


@pytest.mark.asyncio
async def test_consumer_moves_exhausted_message_to_dlq(monkeypatch):
    _use_consumer_settings(monkeypatch, consumer_max_retries=3)
    published = _capture_publish(monkeypatch, "app.consumer.broker.publish")
    _stub_process(monkeypatch, PaymentLockedError("still busy"))
    body = {"payment_id": str(uuid4()), "amount": "10.00"}

    await on_payment_new(body, _Message(headers={"x-retry-count": 2}))

    assert "error" not in body
    assert len(published) == 1
    dead, kwargs = published[0]
    assert kwargs["queue"] is payments_dlq
    assert dead["payment_id"] == body["payment_id"]
    assert dead["amount"] == "10.00"
    assert dead["attempts"] == 3
    assert "still busy" in dead["error"]


@pytest.mark.asyncio
async def test_consumer_first_failure_goes_to_dlq_when_retries_disabled(monkeypatch):
    _use_consumer_settings(monkeypatch, consumer_max_retries=1)
    published = _capture_publish(monkeypatch, "app.consumer.broker.publish")
    _stub_process(monkeypatch, RuntimeError("nope"))

    await on_payment_new({"payment_id": str(uuid4())}, _Message(headers=None))

    dead, kwargs = published[0]
    assert kwargs["queue"] is payments_dlq
    assert dead["attempts"] == 1


@pytest.mark.asyncio
async def test_consumer_rejects_bad_payload_onto_retry_queue(monkeypatch):
    _use_consumer_settings(monkeypatch)
    published = _capture_publish(monkeypatch, "app.consumer.broker.publish")
    seen = _stub_process(monkeypatch)

    await on_payment_new({}, _Message(headers=None))
    await on_payment_new({"payment_id": "not-a-uuid"}, _Message())

    assert seen == []
    assert [body for body, _kwargs in published] == [{}, {"payment_id": "not-a-uuid"}]
    assert all(kwargs["queue"] is payments_retry for _body, kwargs in published)
    assert [kwargs["headers"]["x-retry-count"] for _body, kwargs in published] == [1, 1]


@pytest.mark.asyncio
async def test_consumer_does_not_swallow_broker_outage(monkeypatch):
    _use_consumer_settings(monkeypatch)

    async def broker_down(*_args, **_kwargs):
        raise RuntimeError("broker down")

    monkeypatch.setattr("app.consumer.broker.publish", broker_down)
    _stub_process(monkeypatch, RuntimeError("process down"))

    with pytest.raises(RuntimeError, match="broker down"):
        await on_payment_new({"payment_id": str(uuid4())}, _Message())


@pytest.mark.asyncio
async def test_publish_pending_marks_only_pending_rows(engine, clean_db, relay_sessions, monkeypatch):
    now = datetime.now(timezone.utc)
    pending = _outbox("pending", created_at=now)
    published_row = _outbox("done", created_at=now, status=OutboxStatus.PUBLISHED)
    failed = _outbox("failed", created_at=now, status=OutboxStatus.FAILED)
    await _insert_outbox(engine, pending, published_row, failed)
    published = _capture_publish(monkeypatch, "app.services.outbox_relay.broker.publish")

    count = await publish_pending_outbox()

    assert count == 1
    body, kwargs = published[0]
    assert body == {"marker": "pending"}
    assert kwargs["exchange"] is payments_exchange
    assert kwargs["routing_key"] == get_settings().payments_routing_key

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        row = await session.scalar(select(Outbox).where(Outbox.payload["marker"].astext == "pending"))
        assert row is not None
        assert row.status == OutboxStatus.PUBLISHED
        assert row.attempts == 1
        assert row.published_at is not None


@pytest.mark.asyncio
async def test_publish_failure_stays_pending_until_attempt_limit(
    engine, clean_db, relay_sessions, monkeypatch
):
    row = _outbox("flaky", attempts=9)
    await _insert_outbox(engine, row)

    async def broker_down(*_args, **_kwargs):
        raise RuntimeError("rabbit down")

    monkeypatch.setattr("app.services.outbox_relay.broker.publish", broker_down)

    assert await publish_pending_outbox() == 0

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        stored = await session.scalar(select(Outbox).where(Outbox.payload["marker"].astext == "flaky"))
        assert stored is not None
        assert stored.status == OutboxStatus.FAILED
        assert stored.attempts == 10
        assert stored.published_at is None

    assert await publish_pending_outbox() == 0


@pytest.mark.asyncio
async def test_publish_continues_after_one_row_fails(
    engine, clean_db, relay_sessions, monkeypatch
):
    now = datetime.now(timezone.utc)
    older = _outbox("older", created_at=now - timedelta(seconds=5))
    newer = _outbox("newer", created_at=now)
    await _insert_outbox(engine, newer, older)

    async def fake_publish(body, **_kwargs):
        if body["marker"] == "older":
            raise RuntimeError("temporary")

    monkeypatch.setattr("app.services.outbox_relay.broker.publish", fake_publish)

    assert await publish_pending_outbox() == 1

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        rows = {
            row.payload["marker"]: row
            for row in (await session.scalars(select(Outbox))).all()
        }
        assert rows["older"].status == OutboxStatus.PENDING
        assert rows["older"].attempts == 1
        assert rows["newer"].status == OutboxStatus.PUBLISHED
        assert rows["newer"].attempts == 1


@pytest.mark.asyncio
async def test_lock_pending_skips_locked_rows_and_respects_limit(engine, clean_db):
    now = datetime.now(timezone.utc)
    first = _outbox("1", created_at=now - timedelta(seconds=3))
    second = _outbox("2", created_at=now - timedelta(seconds=2))
    third = _outbox("3", created_at=now)
    await _insert_outbox(engine, third, first, second)

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as holder:
        locked = await outbox_repo.lock_pending(holder, limit=2)
        assert [row.payload["marker"] for row in locked] == ["1", "2"]

        async with session_factory() as other:
            skipped = await outbox_repo.lock_pending(other, limit=50)
            assert [row.payload["marker"] for row in skipped] == ["3"]

        await holder.rollback()

    async with session_factory() as session:
        again = await outbox_repo.lock_pending(session, limit=50)
        assert [row.payload["marker"] for row in again] == ["1", "2", "3"]


@pytest.mark.asyncio
async def test_relay_loop_stops_immediately_when_already_stopped(monkeypatch):
    calls = {"n": 0}

    async def publish():
        calls["n"] += 1
        return 0

    monkeypatch.setattr("app.services.outbox_relay.publish_pending_outbox", publish)
    stop = asyncio.Event()
    stop.set()

    await outbox_relay_loop(stop)
    assert calls["n"] == 0


@pytest.mark.asyncio
async def test_relay_loop_survives_iteration_errors(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "outbox_poll_interval_sec", 0.01)
    calls = {"n": 0}
    stop = asyncio.Event()

    async def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("db down")
        stop.set()
        return 4

    monkeypatch.setattr("app.services.outbox_relay.publish_pending_outbox", flaky)

    await asyncio.wait_for(outbox_relay_loop(stop), timeout=2)
    assert calls["n"] == 2
