import uuid

import pytest
from httpx import AsyncClient


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
