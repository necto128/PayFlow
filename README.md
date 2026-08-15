# PayFlow

Асинхронный сервис приёма и обработки платежей.

```text
API ──► PostgreSQL (payment + outbox)
              │
              ▼
        outbox_worker ──► RabbitMQ
                              │
                              ▼
                         consumer
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
              status update         webhook POST
```

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

| Сервис | URL |
|--------|-----|
| API / Swagger | http://localhost:8080/docs |
| RabbitMQ UI | http://localhost:15672 |

Локально без Docker:

```bash
cp config.example.yml config.yml
cp .env.example .env
poetry install
alembic upgrade head
poetry run uvicorn src.main:app --reload --port 8080
```

Отдельно: `poetry run python -m src.infra.outbox` и `poetry run python -m src.infra.messaging`.

---

## Стек

| Слой | Технологии |
|------|------------|
| API | FastAPI, Dishka |
| Persistence | SQLAlchemy 2 (async), PostgreSQL, Alembic |
| Messaging | RabbitMQ, FastStream |
| Tooling | Poetry, Docker Compose, Ruff, pytest |

---

## Как устроен поток

1. `POST /api/v1/payments/` создаёт платёж `PENDING` и outbox-событие в одной транзакции.
2. `outbox_worker` публикует unpublished-записи в RabbitMQ (`FOR UPDATE SKIP LOCKED`).
3. Consumer на `payments.new` эмулирует шлюз, в одной транзакции ставит `SUCCESS`/`FAILED`, `processed_at` и (если есть URL) кладёт webhook-событие в outbox.
4. Consumer на `payments.webhooks` шлёт HTTP POST получателю.

Доставка событий — **at-least-once**. Повторная обработка безопасна: статус меняется только из `PENDING` под `SELECT FOR UPDATE`.

### Retry / DLQ

Рабочая очередь → при `nack(requeue=False)` в retry (TTL) → обратно в рабочую.  
После `MAX_RETRIES` сообщение уходит в dead-очередь (`payments.new.dead` / `payments.webhooks.dead`).

---

## API

Авторизация: заголовок `X-API-Key` (значение из `.env`, по умолчанию в примере — `SecretKey`).

### Создать платёж

```bash
curl -s -X POST http://localhost:8080/api/v1/payments/ \
  -H "Content-Type: application/json" \
  -H "X-API-Key: SecretKey" \
  -H "Idempotency-Key: 7e4a99b8-bb7d-4af6-9dbf-8f62d463bafb" \
  -d '{
    "amount": 100.00,
    "currency": "USD",
    "description": "Order #123",
    "metadata": {"user_id": 1},
    "webhook_url": "https://example.com/webhook"
  }'
```

Ответ `202`:

```json
{
  "id": "5bfc4131-5a58-48d0-a0d3-c2bb16b16dd8",
  "amount": "100.00",
  "currency": "USD",
  "status": "PENDING",
  "created_at": "2026-06-19T18:15:00.577431Z"
}
```

Тот же `Idempotency-Key` возвращает существующий платёж без второго outbox-события.

### Получить платёж

```bash
curl -s http://localhost:8080/api/v1/payments/<payment_id> \
  -H "X-API-Key: SecretKey"
```

### Webhook payload

```http
POST <webhook_url>
X-Event-Id: <uuid>
Content-Type: application/json

{"payment_id": "<uuid>", "status": "SUCCESS"}
```

---

## Структура кода

```text
src/
  app/        # routes, DTO, use cases
  domains/    # сущности и правила платежей
  infra/      # ORM, RabbitMQ, outbox, consumer
  core/       # settings, security
```

Compose-сервисы: `app`, `outbox_worker`, `consumer_worker`, `migrations`, `postgres`, `rabbitmq`.  
В контейнерах конфиг — `config.docker.yml` (`CONFIG_FILE`).

---

## Миграции и тесты

```bash
alembic revision --autogenerate -m "change"
alembic upgrade head

poetry run pytest tests/units
poetry run pytest tests/integrations   # нужен совместимый TESTING.DB_URL (PostgreSQL)
```

В Docker миграции гоняет сервис `migrations` до старта API.
