import uuid
from datetime import datetime, timezone

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.payment import PaymentStatus
from app.repositories import payment as payment_repo


@pytest.mark.asyncio
async def test_health(client: AsyncClient):
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_create_payment_requires_api_key(client: AsyncClient, payment_payload: dict):
    response = await client.post(
        "/api/v1/payments",
        json=payment_payload,
        headers={"Idempotency-Key": str(uuid.uuid4())},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_payment_requires_idempotency_key(
    client: AsyncClient,
    payment_payload: dict,
):
    response = await client.post(
        "/api/v1/payments",
        json=payment_payload,
        headers={"X-API-Key": "dev-api-key-change-me"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_create_payment_202(
    client: AsyncClient,
    api_headers: dict[str, str],
    payment_payload: dict,
):
    response = await client.post(
        "/api/v1/payments",
        json=payment_payload,
        headers=api_headers,
    )
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "pending"
    assert "payment_id" in body
    assert "created_at" in body


@pytest.mark.asyncio
async def test_idempotency_returns_same_payment(
    client: AsyncClient,
    payment_payload: dict,
):
    key = f"idem-{uuid.uuid4()}"
    headers = {"X-API-Key": "dev-api-key-change-me", "Idempotency-Key": key}

    first = await client.post("/api/v1/payments", json=payment_payload, headers=headers)
    second = await client.post("/api/v1/payments", json=payment_payload, headers=headers)

    assert first.status_code == 202
    assert second.status_code == 200
    assert first.json()["payment_id"] == second.json()["payment_id"]


@pytest.mark.asyncio
async def test_idempotency_conflict_returns_409(
    client: AsyncClient,
    payment_payload: dict,
):
    key = f"idem-{uuid.uuid4()}"
    headers = {"X-API-Key": "dev-api-key-change-me", "Idempotency-Key": key}

    first = await client.post("/api/v1/payments", json=payment_payload, headers=headers)
    assert first.status_code == 202

    conflict_payload = {**payment_payload, "amount": "1500.01"}
    second = await client.post("/api/v1/payments", json=conflict_payload, headers=headers)

    assert second.status_code == 409
    assert second.json()["detail"] == (
        "Idempotency-Key already used with a different request body"
    )


@pytest.mark.asyncio
async def test_get_payment(
    client: AsyncClient,
    api_headers: dict[str, str],
    payment_payload: dict,
):
    created = await client.post(
        "/api/v1/payments",
        json=payment_payload,
        headers=api_headers,
    )
    payment_id = created.json()["payment_id"]

    response = await client.get(
        f"/api/v1/payments/{payment_id}",
        headers={"X-API-Key": "dev-api-key-change-me"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == payment_id
    assert body["amount"] == "1500.00"
    assert body["currency"] == "RUB"
    assert body["metadata"] == {"order_id": "42"}
    assert body["status"] == "pending"
    assert body["webhook_url"] == "https://example.com/webhook"


@pytest.mark.asyncio
async def test_get_payment_not_found(client: AsyncClient):
    response = await client.get(
        f"/api/v1/payments/{uuid.uuid4()}",
        headers={"X-API-Key": "dev-api-key-change-me"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_create_payment_validation_error(
    client: AsyncClient,
    api_headers: dict[str, str],
):
    response = await client.post(
        "/api/v1/payments",
        json={
            "amount": "-1",
            "currency": "RUB",
            "description": "bad",
            "webhook_url": "https://example.com/h",
        },
        headers=api_headers,
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_wrong_api_key_is_rejected(client: AsyncClient, payment_payload: dict):
    response = await client.post(
        "/api/v1/payments",
        json=payment_payload,
        headers={"X-API-Key": "nope", "Idempotency-Key": str(uuid.uuid4())},
    )
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or missing API key"

    created = await client.post(
        "/api/v1/payments",
        json=payment_payload,
        headers={
            "X-API-Key": "dev-api-key-change-me",
            "Idempotency-Key": str(uuid.uuid4()),
        },
    )
    hidden = await client.get(f"/api/v1/payments/{created.json()['payment_id']}")
    assert hidden.status_code == 401


@pytest.mark.asyncio
async def test_blank_idempotency_key_is_rejected(client: AsyncClient, payment_payload: dict):
    response = await client.post(
        "/api/v1/payments",
        json=payment_payload,
        headers={"X-API-Key": "dev-api-key-change-me", "Idempotency-Key": "   "},
    )
    assert response.status_code == 422
    assert response.json()["detail"] == "Idempotency-Key must not be empty"


@pytest.mark.asyncio
async def test_idempotency_key_is_stripped(client: AsyncClient, payment_payload: dict):
    key = f"order-{uuid.uuid4()}"
    first = await client.post(
        "/api/v1/payments",
        json=payment_payload,
        headers={"X-API-Key": "dev-api-key-change-me", "Idempotency-Key": f"  {key}  "},
    )
    second = await client.post(
        "/api/v1/payments",
        json=payment_payload,
        headers={"X-API-Key": "dev-api-key-change-me", "Idempotency-Key": key},
    )
    assert first.status_code == 202
    assert second.status_code == 200
    assert first.json()["payment_id"] == second.json()["payment_id"]


@pytest.mark.asyncio
async def test_idempotency_treats_equivalent_amounts_as_same_body(
    client: AsyncClient,
    api_headers: dict[str, str],
    payment_payload: dict,
):
    first = await client.post("/api/v1/payments", json=payment_payload, headers=api_headers)
    second = await client.post(
        "/api/v1/payments",
        json={**payment_payload, "amount": "1500"},
        headers=api_headers,
    )
    assert first.status_code == 202
    assert second.status_code == 200
    assert first.json()["payment_id"] == second.json()["payment_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"amount": "10.00"},
        {"currency": "USD"},
        {"description": "Other order"},
        {"metadata": {"order_id": "43"}},
        {"webhook_url": "https://example.com/other"},
    ],
)
async def test_idempotency_conflict_for_each_changed_field(
    client: AsyncClient,
    api_headers: dict[str, str],
    payment_payload: dict,
    changes: dict,
):
    first = await client.post("/api/v1/payments", json=payment_payload, headers=api_headers)
    assert first.status_code == 202

    second = await client.post(
        "/api/v1/payments",
        json={**payment_payload, **changes},
        headers=api_headers,
    )
    assert second.status_code == 409


@pytest.mark.asyncio
async def test_replay_returns_current_status_and_conflict_does_not_mutate(
    client: AsyncClient,
    engine,
    api_headers: dict[str, str],
    payment_payload: dict,
):
    created = await client.post("/api/v1/payments", json=payment_payload, headers=api_headers)
    payment_id = uuid.UUID(created.json()["payment_id"])

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        payment = await payment_repo.get_by_id(session, payment_id)
        assert payment is not None
        payment.status = PaymentStatus.SUCCEEDED
        payment.processed_at = datetime.now(timezone.utc)
        await session.commit()

    replay = await client.post("/api/v1/payments", json=payment_payload, headers=api_headers)
    assert replay.status_code == 200
    assert replay.json()["payment_id"] == str(payment_id)
    assert replay.json()["status"] == "succeeded"

    conflict = await client.post(
        "/api/v1/payments",
        json={**payment_payload, "amount": "1.00"},
        headers=api_headers,
    )
    assert conflict.status_code == 409

    stored = await client.get(
        f"/api/v1/payments/{payment_id}",
        headers={"X-API-Key": "dev-api-key-change-me"},
    )
    assert stored.status_code == 200
    body = stored.json()
    assert body["status"] == "succeeded"
    assert body["amount"] == "1500.00"


@pytest.mark.asyncio
async def test_get_payment_rejects_invalid_id(client: AsyncClient):
    response = await client.get(
        "/api/v1/payments/not-a-uuid",
        headers={"X-API-Key": "dev-api-key-change-me"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"amount": "0"},
        {"amount": "1500.001"},
        {"currency": "GBP"},
        {"description": ""},
        {"webhook_url": "not-a-url"},
    ],
)
async def test_create_payment_rejects_invalid_fields(
    client: AsyncClient,
    api_headers: dict[str, str],
    payment_payload: dict,
    changes: dict,
):
    response = await client.post(
        "/api/v1/payments",
        json={**payment_payload, **changes},
        headers=api_headers,
    )
    assert response.status_code == 422

