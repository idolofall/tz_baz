import asyncio
import logging
from typing import Any

import httpx

from app.core.config import get_settings

logger = logging.getLogger(__name__)


async def send_webhook(url: str, payload: dict[str, Any]) -> None:
    """Send webhook with exponential backoff retries."""
    settings = get_settings()
    last_error: Exception | None = None

    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(1, settings.webhook_max_retries + 1):
            try:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                logger.info("Webhook delivered to %s (attempt %s)", url, attempt)
                return
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Webhook attempt %s/%s failed for %s: %s",
                    attempt,
                    settings.webhook_max_retries,
                    url,
                    exc,
                )
                if attempt < settings.webhook_max_retries:
                    delay = settings.webhook_retry_base_delay_sec * (2 ** (attempt - 1))
                    await asyncio.sleep(delay)

    raise RuntimeError(f"Webhook delivery failed after {settings.webhook_max_retries} attempts") from last_error
