import pytest
import respx
from httpx import Response

from app.core.config import get_settings
from app.services.webhook import send_webhook


@pytest.fixture(autouse=True)
def _fast_webhook_retries(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("WEBHOOK_MAX_RETRIES", "3")
    monkeypatch.setenv("WEBHOOK_RETRY_BASE_DELAY_SEC", "0.01")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.mark.asyncio
@respx.mock
async def test_webhook_success_first_try():
    route = respx.post("https://hooks.example/ok").mock(return_value=Response(200))
    await send_webhook("https://hooks.example/ok", {"payment_id": "1", "status": "succeeded"})
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_webhook_retries_then_succeeds():
    route = respx.post("https://hooks.example/retry").mock(
        side_effect=[
            Response(500),
            Response(500),
            Response(200),
        ]
    )
    await send_webhook(
        "https://hooks.example/retry",
        {"payment_id": "2", "status": "failed"},
    )
    assert route.call_count == 3


@pytest.mark.asyncio
@respx.mock
async def test_webhook_fails_after_max_retries():
    route = respx.post("https://hooks.example/fail").mock(return_value=Response(500))
    with pytest.raises(RuntimeError, match="Webhook delivery failed"):
        await send_webhook("https://hooks.example/fail", {"payment_id": "3"})
    assert route.call_count == 3
