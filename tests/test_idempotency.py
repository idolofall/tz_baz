import asyncio
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.outbox import Outbox
from app.models.payment import Payment, PaymentStatus
from app.schemas.payment import PaymentCreate
from app.services.payment import IdempotencyConflict, create_payment


def _payload(**overrides) -> PaymentCreate:
    fields = {
        "amount": Decimal("10.00"),
        "currency": "EUR",
        "description": "dup",
        "metadata": {"k": "v"},
        "webhook_url": "https://example.com/hook",
    }
    fields.update(overrides)
    return PaymentCreate(**fields)


def _integrity(message: str) -> IntegrityError:
    return IntegrityError("INSERT", {}, Exception(message))


class _Session:
    def __init__(self, flush_error: Exception | None = None):
        self.flush_error = flush_error
        self.rolled_back = False
        self.committed = False
        self.events: list[str] = []

    def add(self, _obj) -> None:
        self.events.append("add")

    async def flush(self) -> None:
        self.events.append("flush")
        if self.flush_error is not None:
            raise self.flush_error

    async def rollback(self) -> None:
        self.rolled_back = True
        self.events.append("rollback")

    async def commit(self) -> None:
        self.committed = True

    async def refresh(self, _obj) -> None:
        return None


@pytest.mark.asyncio
async def test_equivalent_amount_reuses_payment(engine, clean_db):
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    key = f"scale-{uuid4()}"

    async with session_factory() as session:
        first, created = await create_payment(session, _payload(), key)
        assert created is True

    async with session_factory() as session:
        second, created = await create_payment(
            session,
            _payload(amount=Decimal("10.0")),
            key,
        )
        assert created is False
        assert second.id == first.id


@pytest.mark.asyncio
async def test_different_keys_create_independent_payments(engine, clean_db):
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    payload = _payload()

    async with session_factory() as session:
        first, created_first = await create_payment(session, payload, f"a-{uuid4()}")
        assert created_first is True

    async with session_factory() as session:
        second, created_second = await create_payment(session, payload, f"b-{uuid4()}")
        assert created_second is True
        assert first.id != second.id

        count = await session.scalar(select(func.count()).select_from(Outbox))
        assert count == 2


@pytest.mark.asyncio
async def test_concurrent_same_key_creates_one_payment_and_one_outbox(engine, clean_db):
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    key = f"race-{uuid4()}"
    payload = _payload()

    async def once():
        async with session_factory() as session:
            return await create_payment(session, payload, key)

    results = await asyncio.gather(*[once() for _ in range(5)])
    ids = {payment.id for payment, _created in results}
    assert len(ids) == 1
    assert sum(created for _payment, created in results) == 1

    async with session_factory() as session:
        outbox_count = await session.scalar(
            select(func.count()).select_from(Outbox).where(Outbox.aggregate_id == next(iter(ids)))
        )
        assert outbox_count == 1


@pytest.mark.asyncio
async def test_concurrent_same_key_different_body_conflicts(engine, clean_db):
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    key = f"race-conflict-{uuid4()}"

    async def once(amount: str):
        async with session_factory() as session:
            return await create_payment(session, _payload(amount=Decimal(amount)), key)

    results = await asyncio.gather(
        once("10.00"),
        once("11.00"),
        once("12.00"),
        return_exceptions=True,
    )
    created = [item for item in results if not isinstance(item, Exception)]
    conflicts = [item for item in results if isinstance(item, Exception)]

    assert len(created) == 1
    _payment, was_created = created[0]
    assert was_created is True
    assert conflicts
    assert all(isinstance(item, IdempotencyConflict) for item in conflicts)

    async with session_factory() as session:
        stored = await session.scalar(select(func.count()).select_from(Payment))
        assert stored == 1


@pytest.mark.asyncio
async def test_unique_conflict_returns_winner_without_second_outbox(monkeypatch):
    payload = _payload()
    existing = Payment(
        amount=payload.amount,
        currency=payload.currency,
        description=payload.description,
        metadata_=dict(payload.metadata),
        status=PaymentStatus.PENDING,
        idempotency_key="race-key",
        webhook_url=str(payload.webhook_url),
    )
    answers = [None, existing]

    async def get_by_key(_session, _key):
        return answers.pop(0)

    monkeypatch.setattr(
        "app.services.payment.payment_repo.get_by_idempotency_key",
        get_by_key,
    )
    session = _Session(
        _integrity('duplicate key value violates unique constraint "ix_payments_idempotency_key"')
    )

    payment, created = await create_payment(session, payload, "race-key")

    assert created is False
    assert payment is existing
    assert session.rolled_back is True
    assert session.committed is False
    assert session.events == ["add", "flush", "rollback"]


@pytest.mark.asyncio
async def test_unique_conflict_with_different_body_raises(monkeypatch):
    payload = _payload()
    existing = Payment(
        amount=Decimal("99.00"),
        currency=payload.currency,
        description=payload.description,
        metadata_=dict(payload.metadata),
        status=PaymentStatus.PENDING,
        idempotency_key="race-key",
        webhook_url=str(payload.webhook_url),
    )
    answers = [None, existing]

    async def get_by_key(_session, _key):
        return answers.pop(0)

    monkeypatch.setattr(
        "app.services.payment.payment_repo.get_by_idempotency_key",
        get_by_key,
    )
    session = _Session(_integrity("unique constraint ix_payments_idempotency_key"))

    with pytest.raises(IdempotencyConflict):
        await create_payment(session, payload, "race-key")
    assert session.rolled_back is True
    assert session.committed is False


@pytest.mark.asyncio
async def test_unique_conflict_without_winner_reraises(monkeypatch):
    async def get_by_key(_session, _key):
        return None

    monkeypatch.setattr(
        "app.services.payment.payment_repo.get_by_idempotency_key",
        get_by_key,
    )
    session = _Session(_integrity("idempotency_key already exists"))

    with pytest.raises(IntegrityError):
        await create_payment(session, _payload(), "race-key")
    assert session.rolled_back is True


@pytest.mark.asyncio
async def test_unrelated_integrity_error_is_not_treated_as_idempotency(monkeypatch):
    calls = {"n": 0}

    async def get_by_key(_session, _key):
        calls["n"] += 1
        return None

    monkeypatch.setattr(
        "app.services.payment.payment_repo.get_by_idempotency_key",
        get_by_key,
    )
    session = _Session(_integrity('duplicate key value violates unique constraint "payments_pkey"'))

    with pytest.raises(IntegrityError):
        await create_payment(session, _payload(), "race-key")
    assert calls["n"] == 1
    assert session.rolled_back is False
    assert session.committed is False
