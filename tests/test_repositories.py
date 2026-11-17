from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.payment import Payment, PaymentStatus
from app.repositories import payment as payment_repo


@pytest.mark.asyncio
async def test_try_claim_captures_pending_once(engine, clean_db):
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    payment = Payment(
        amount=Decimal("1.00"),
        currency="RUB",
        description="claim",
        metadata_={},
        status=PaymentStatus.PENDING,
        idempotency_key=f"claim-{uuid4()}",
        webhook_url="https://example.com/h",
    )

    async with session_factory() as session:
        payment_repo.add(session, payment)
        await session.commit()
        await session.refresh(payment)
        payment_id = payment.id

    async with session_factory() as session:
        first = await payment_repo.try_claim(session, payment_id, stale_after_sec=10)
        await session.commit()
        assert first is not None
        assert first.status == PaymentStatus.PROCESSING
        assert first.processing_started_at is not None

    async with session_factory() as session:
        second = await payment_repo.try_claim(session, payment_id, stale_after_sec=10)
        await session.commit()
        assert second is None


@pytest.mark.asyncio
async def test_try_claim_reclaims_stale_processing(engine, clean_db):
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    payment = Payment(
        amount=Decimal("1.00"),
        currency="EUR",
        description="stale",
        metadata_={},
        status=PaymentStatus.PROCESSING,
        idempotency_key=f"stale-{uuid4()}",
        webhook_url="https://example.com/h",
        processing_started_at=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    async with session_factory() as session:
        payment_repo.add(session, payment)
        await session.commit()
        await session.refresh(payment)
        payment_id = payment.id

    async with session_factory() as session:
        claimed = await payment_repo.try_claim(session, payment_id, stale_after_sec=10)
        await session.commit()
        assert claimed is not None
        assert claimed.status == PaymentStatus.PROCESSING


async def _save_payment(engine, **overrides):
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    fields = {
        "amount": Decimal("1.00"),
        "currency": "RUB",
        "description": "repo",
        "metadata_": {},
        "status": PaymentStatus.PENDING,
        "idempotency_key": f"repo-{uuid4()}",
        "webhook_url": "https://example.com/h",
    }
    fields.update(overrides)
    payment = Payment(**fields)
    async with session_factory() as session:
        payment_repo.add(session, payment)
        await session.commit()
        await session.refresh(payment)
        return payment


@pytest.mark.asyncio
async def test_try_claim_skips_fresh_processing(engine, clean_db):
    payment = await _save_payment(
        engine,
        status=PaymentStatus.PROCESSING,
        processing_started_at=datetime.now(timezone.utc),
    )
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        claimed = await payment_repo.try_claim(session, payment.id, stale_after_sec=10)
        assert claimed is None


@pytest.mark.asyncio
async def test_try_claim_skips_processing_without_started_at(engine, clean_db):
    payment = await _save_payment(engine, status=PaymentStatus.PROCESSING)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        claimed = await payment_repo.try_claim(session, payment.id, stale_after_sec=0)
        assert claimed is None


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_try_claim_skips_terminal_payment(engine, clean_db, status):
    payment = await _save_payment(
        engine,
        status=status,
        processing_started_at=datetime.now(timezone.utc) - timedelta(days=1),
    )
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        claimed = await payment_repo.try_claim(session, payment.id, stale_after_sec=10)
        await session.commit()
        assert claimed is None
        stored = await payment_repo.get_by_id(session, payment.id)
        assert stored is not None
        assert stored.status == status


@pytest.mark.asyncio
async def test_mark_processed_only_from_processing(engine, clean_db):
    payment = await _save_payment(engine)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_factory() as session:
        skipped = await payment_repo.mark_processed(session, payment.id, PaymentStatus.SUCCEEDED)
        await session.commit()
        assert skipped is None

    async with session_factory() as session:
        claimed = await payment_repo.try_claim(session, payment.id, stale_after_sec=10)
        await session.commit()
        assert claimed is not None

    async with session_factory() as session:
        processed = await payment_repo.mark_processed(
            session, payment.id, PaymentStatus.SUCCEEDED
        )
        assert processed is not None
        overwritten = await payment_repo.mark_processed(
            session, payment.id, PaymentStatus.FAILED
        )
        await session.commit()
        assert overwritten is None

    async with session_factory() as session:
        stored = await payment_repo.get_by_id(session, payment.id)
        assert stored is not None
        assert stored.status == PaymentStatus.SUCCEEDED
        assert stored.processed_at is not None


@pytest.mark.asyncio
async def test_mark_webhook_delivered_is_idempotent(engine, clean_db):
    payment = await _save_payment(engine)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_factory() as session:
        await payment_repo.mark_webhook_delivered(session, payment.id)
        await session.commit()

    async with session_factory() as session:
        stored = await payment_repo.get_by_id(session, payment.id)
        assert stored is not None
        delivered_at = stored.webhook_delivered_at
        assert delivered_at is not None
        stored.webhook_delivered_at = datetime(2020, 1, 1, tzinfo=timezone.utc)
        await session.commit()

    async with session_factory() as session:
        await payment_repo.mark_webhook_delivered(session, payment.id)
        await session.commit()

    async with session_factory() as session:
        stored = await payment_repo.get_by_id(session, payment.id)
        assert stored is not None
        assert stored.webhook_delivered_at == datetime(2020, 1, 1, tzinfo=timezone.utc)
