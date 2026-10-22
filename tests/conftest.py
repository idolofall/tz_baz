import os
import uuid
from collections.abc import AsyncGenerator

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Force test settings before app imports cache settings
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+asyncpg://payments:payments@localhost:5433/payments",
)
os.environ.setdefault("API_KEY", "dev-api-key-change-me")
os.environ.setdefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

from app.api.payments import router as payments_router
from app.core.config import get_settings
from app.core.db import get_session
from app.models import Outbox, Payment  # noqa: F401


@pytest.fixture(scope="session")
def settings():
    get_settings.cache_clear()
    return get_settings()


@pytest.fixture
async def engine(settings):
    eng = create_async_engine(settings.database_url, pool_pre_ping=True)
    yield eng
    await eng.dispose()


@pytest.fixture
async def session(engine) -> AsyncGenerator[AsyncSession, None]:
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as db:
        yield db
        await db.rollback()


@pytest.fixture
async def clean_db(engine) -> AsyncGenerator[None, None]:
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE outbox, payments RESTART IDENTITY CASCADE"))
    yield
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE outbox, payments RESTART IDENTITY CASCADE"))


@pytest.fixture
async def client(engine, clean_db) -> AsyncGenerator[AsyncClient, None]:
    """API test client without RabbitMQ lifespan."""
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def override_get_session() -> AsyncGenerator[AsyncSession, None]:
        async with session_factory() as db:
            yield db

    test_app = FastAPI()
    test_app.include_router(payments_router)

    @test_app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    test_app.dependency_overrides[get_session] = override_get_session

    transport = ASGITransport(app=test_app, raise_app_exceptions=True)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    test_app.dependency_overrides.clear()


@pytest.fixture
def api_headers() -> dict[str, str]:
    return {
        "X-API-Key": "dev-api-key-change-me",
        "Idempotency-Key": f"test-{uuid.uuid4()}",
    }


@pytest.fixture
def payment_payload() -> dict:
    return {
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Test order",
        "metadata": {"order_id": "42"},
        "webhook_url": "https://example.com/webhook",
    }
