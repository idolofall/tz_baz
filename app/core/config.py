from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5433/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: str = "dev-api-key-change-me"

    outbox_poll_interval_sec: float = 1.0

    webhook_max_retries: int = 3
    webhook_retry_base_delay_sec: float = 1.0

    payment_process_min_sec: float = 2.0
    payment_process_max_sec: float = 5.0
    payment_success_rate: float = 0.9

    consumer_max_retries: int = 3

    payments_exchange: str = "payments"
    payments_routing_key: str = "payments.new"
    payments_queue: str = "payments.new"
    payments_dlq: str = "payments.new.dlq"


@lru_cache
def get_settings() -> Settings:
    return Settings()
