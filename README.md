# Payment Processing Service

Backend-сервис обработки платежей на **FastAPI** и **RabbitMQ** с **Outbox Pattern**.

---

# Стек технологий

* FastAPI
* SQLAlchemy 2.0 (async)
* PostgreSQL
* RabbitMQ / FastStream
* Alembic
* Dishka
* Poetry
* Docker

---

# Возможности

* Создание и получение платежа
* Идемпотентность через `Idempotency-Key`
* Outbox Pattern для гарантированной публикации событий
* Асинхронная обработка через RabbitMQ
* Эмуляция внешнего платежного шлюза
* Webhook после обработки платежа
* Retry через DLX + TTL retry-очереди
* Dead Letter Queue для исчерпавших retry сообщений
* Unit и integration тесты

---

## Структура проекта

```
src/
├── app/           # API, DTO, use cases
├── domains/       # доменная модель платежей
├── infra/         # БД, RabbitMQ, outbox worker, consumer
├── core/          # settings, security, базовые интерфейсы
├── containers.py
├── bootstrap.py
└── main.py
```

## Подготовка

```bash
cp config.example.yml config.yml
cp .env.example .env
```

Для Docker Compose достаточно `.env`; конфигурация берётся из `config.docker.yml`.

---

## Запуск через Docker Compose

```bash
cp .env.example .env
docker compose up --build
```

Доступно:

* FastAPI — http://localhost:8080
* Swagger — http://localhost:8080/docs
* RabbitMQ Management — http://localhost:15672 (user/pass из `.env`)

Сервисы:

* `app` — REST API
* `outbox_worker` — публикация outbox → RabbitMQ
* `consumer_worker` — обработка платежей и webhook
* `migrations` — `alembic upgrade head`
* `postgres`, `rabbitmq`

---

# Миграции

```bash
alembic revision --autogenerate -m "init"
alembic upgrade head
```

В Docker миграции применяются сервисом `migrations` перед стартом API.

---

# REST API

## Создание платежа

### POST /api/v1/payments/

Headers:

```http
Idempotency-Key: 7e4a99b8-bb7d-4af6-9dbf-8f62d463bafb
X-API-Key: SecretKey
```

Body:

```json
{
  "amount": 100.00,
  "currency": "USD",
  "description": "Order #123",
  "metadata": {
    "user_id": 1
  },
  "webhook_url": "https://example.com/webhook"
}
```

Response `202`:

```json
{
  "id": "5bfc4131-5a58-48d0-a0d3-c2bb16b16dd8",
  "amount": 100.00,
  "currency": "USD",
  "status": "PENDING",
  "created_at": "2026-06-19T18:15:00.577431Z"
}
```

Повторный запрос с тем же `Idempotency-Key` возвращает уже созданный платёж без дублирования события.

## Получение платежа

### GET /api/v1/payments/{payment_id}

Headers:

```http
X-API-Key: SecretKey
```

---

# Outbox Pattern

При создании платежа событие пишется в таблицу `outbox` в одной транзакции с записью платежа.
`outbox_worker` читает unpublished-сообщения (`FOR UPDATE SKIP LOCKED`), публикует в RabbitMQ и помечает как опубликованные.

Повторная доставка возможна при сбое после publish до commit — consumer идемпотентен (обработка только `PENDING` + `SELECT FOR UPDATE`).

---

# Обработка платежей

Consumer читает `payments.new`, эмулирует шлюз и в **одной транзакции**:

* меняет статус на `SUCCESS` / `FAILED`
* выставляет `processed_at`
* при наличии `webhook_url` кладёт событие webhook в outbox

---

# Webhook

После обработки outbox публикует событие в `payments.webhooks`.
Consumer отправляет HTTP POST:

```json
{
  "payment_id": "<uuid>",
  "status": "SUCCESS"
}
```

Header: `X-Event-Id`.

---

# Retry и DLQ

Схема брокера:

* рабочие очереди (`payments.new`, `payments.webhooks`) с DLX → retry-exchange
* retry-очереди с TTL (`RETRY_TTL_MS`) и DLX обратно в рабочую очередь
* при `nack(requeue=False)` сообщение уходит в retry, затем возвращается с заголовком `x-death`
* после `MAX_RETRIES` consumer публикует сообщение в DLQ (`payments.new.dead` / `payments.webhooks.dead`) и делает `ack`

---

# Тестирование

```bash
cp config.example.yml config.yml
cp .env.example .env
poetry install
poetry run pytest
```

```bash
poetry run pytest tests/units
poetry run pytest tests/integrations
```
