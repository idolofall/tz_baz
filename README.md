# Payment Processing Service

Асинхронный микросервис процессинга платежей: API принимает платёж, через Outbox публикует событие в RabbitMQ, consumer эмулирует платёжный шлюз и уведомляет клиента webhook'ом.

## Стек

- FastAPI + Pydantic v2
- SQLAlchemy 2.0 (async) + PostgreSQL
- RabbitMQ + FastStream
- Alembic
- Docker Compose

## Быстрый старт

```bash
cp .env.example .env
docker compose up --build
```

Сервисы:

| Сервис   | URL                          |
|----------|------------------------------|
| API      | http://localhost:8000        |
| Swagger  | http://localhost:8000/docs   |
| RabbitMQ | http://localhost:15672 (guest/guest) |

## Переменные окружения

См. [`.env.example`](.env.example). Основные:

- `DATABASE_URL` — PostgreSQL (asyncpg)
- `RABBITMQ_URL` — AMQP URL
- `API_KEY` — статический ключ для заголовка `X-API-Key`
- `CONSUMER_MAX_RETRIES` — попытки обработки сообщения до DLQ (по умолчанию 3)
- `WEBHOOK_MAX_RETRIES` — попытки webhook с экспоненциальной задержкой

## Примеры API

### Создать платёж

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-api-key-change-me" \
  -H "Idempotency-Key: order-1001" \
  -d '{
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order #1001",
    "metadata": {"order_id": "1001"},
    "webhook_url": "https://webhook.site/your-unique-id"
  }'
```

Ответ `202 Accepted`:

```json
{
  "payment_id": "...",
  "status": "pending",
  "created_at": "..."
}
```

Повторный запрос с тем же `Idempotency-Key` не создаёт дубль.

### Получить платёж

```bash
curl -s http://localhost:8000/api/v1/payments/<payment_id> \
  -H "X-API-Key: dev-api-key-change-me"
```

## Архитектура

1. **POST /payments** — в одной транзакции создаются `payments` + запись в `outbox`.
2. **Outbox relay** (фон в API) — публикует pending-события в exchange `payments` (routing key `payments.new`).
3. **Consumer** — читает очередь `payments.new`, эмуляция шлюза (2–5 сек, ~90% success), статус, webhook.
4. При ошибках обработки — до 3 попыток, затем сообщение уходит в `payments.new.dlq`.
5. Webhook — до 3 попыток с экспоненциальной задержкой.

## Тесты

Нужны поднятые Postgres/RabbitMQ из compose (Postgres с хоста: `localhost:5433`):

```bash
docker compose up -d postgres rabbitmq
pip install -e ".[dev]"
pytest -v
```

## Локальный запуск без Docker (опционально)

Нужны локальные PostgreSQL и RabbitMQ.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
cp .env.example .env
# поправьте DATABASE_URL / RABBITMQ_URL на localhost
alembic upgrade head
uvicorn app.main:app --reload
# в другом терминале:
faststream run app.consumer:app
```

## Структура

```
app/
  api/          # HTTP эндпоинты
  core/         # settings, db, auth
  messaging/    # RabbitMQ broker / queues
  models/       # SQLAlchemy модели
  schemas/      # Pydantic схемы
  services/     # бизнес-логика, outbox, webhook
  main.py       # FastAPI + outbox relay
  consumer.py   # FastStream consumer
alembic/        # миграции
```
