from decimal import Decimal
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.outbox import Outbox, OutboxStatus
from app.models.payment import PaymentStatus
from app.schemas.payment import PaymentCreate
from app.services.payment import create_payment


@pytest.mark.asyncio
async def test_create_payment_writes_outbox(engine, clean_db):
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        payload = PaymentCreate(
            amount=Decimal("10.00"),
            currency="USD",
            description="outbox check",
            metadata={"k": "v"},
            webhook_url="https://example.com/hook",
        )
        payment, created = await create_payment(session, payload, "outbox-key-1")
        assert created is True
        assert payment.status == PaymentStatus.PENDING

        result = await session.execute(
            select(Outbox).where(Outbox.aggregate_id == payment.id)
        )
        outbox = result.scalar_one()
        assert outbox.status == OutboxStatus.PENDING
        assert outbox.event_type == "payments.new"
        assert outbox.payload["payment_id"] == str(payment.id)
        assert UUID(outbox.payload["payment_id"]) == payment.id


@pytest.mark.asyncio
async def test_idempotent_create_does_not_duplicate_outbox(engine, clean_db):
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    payload = PaymentCreate(
        amount=Decimal("10.00"),
        currency="EUR",
        description="dup",
        metadata={},
        webhook_url="https://example.com/hook",
    )

    async with session_factory() as session:
        first, created1 = await create_payment(session, payload, "dup-key")
        assert created1 is True

    async with session_factory() as session:
        second, created2 = await create_payment(session, payload, "dup-key")
        assert created2 is False
        assert first.id == second.id

        result = await session.execute(select(Outbox).where(Outbox.aggregate_id == first.id))
        rows = result.scalars().all()
        assert len(rows) == 1
