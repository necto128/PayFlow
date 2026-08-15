from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from src.core.settings import settings

topics = settings.MESSAGING.PAYMENTS.TOPICS
retry_ttl_ms = settings.MESSAGING.RETRY_TTL_MS

payments_exchange = RabbitExchange(name="payments.exchange", type=ExchangeType.DIRECT, durable=True)
retry_exchange = RabbitExchange(name="payments.retry_exchange", type=ExchangeType.DIRECT, durable=True)
dead_exchange = RabbitExchange(name="payments.dead_exchange", type=ExchangeType.DIRECT, durable=True)

new_queue = RabbitQueue(
    name=topics.NEW,
    durable=True,
    routing_key=topics.NEW,
    arguments={
        "x-dead-letter-exchange": retry_exchange.name,
        "x-dead-letter-routing-key": topics.NEW_RETRY,
    },
)

new_retry_queue = RabbitQueue(
    name=topics.NEW_RETRY,
    durable=True,
    routing_key=topics.NEW_RETRY,
    arguments={
        "x-message-ttl": retry_ttl_ms,
        "x-dead-letter-exchange": payments_exchange.name,
        "x-dead-letter-routing-key": topics.NEW,
    },
)

new_dead_queue = RabbitQueue(
    name=topics.NEW_DEAD,
    durable=True,
    routing_key=topics.NEW_DEAD,
)

webhook_queue = RabbitQueue(
    name=topics.WEBHOOKS,
    durable=True,
    routing_key=topics.WEBHOOKS,
    arguments={
        "x-dead-letter-exchange": retry_exchange.name,
        "x-dead-letter-routing-key": topics.WEBHOOKS_RETRY,
    },
)

webhook_retry_queue = RabbitQueue(
    name=topics.WEBHOOKS_RETRY,
    durable=True,
    routing_key=topics.WEBHOOKS_RETRY,
    arguments={
        "x-message-ttl": retry_ttl_ms,
        "x-dead-letter-exchange": payments_exchange.name,
        "x-dead-letter-routing-key": topics.WEBHOOKS,
    },
)

dead_queue = RabbitQueue(
    name=topics.WEBHOOKS_DEAD,
    durable=True,
    routing_key=topics.WEBHOOKS_DEAD,
)


def get_death_count(headers: dict | None) -> int:
    if not headers:
        return 0
    death_header = headers.get("x-death")
    if not death_header:
        return 0
    return sum(int(entry.get("count", 0)) for entry in death_header)


async def setup_rabbit_infrastructure(broker: RabbitBroker) -> None:
    declared_payments_exchange = await broker.declare_exchange(payments_exchange)
    declared_retry_exchange = await broker.declare_exchange(retry_exchange)
    declared_dead_exchange = await broker.declare_exchange(dead_exchange)

    declared_new_queue = await broker.declare_queue(new_queue)
    declared_new_retry_queue = await broker.declare_queue(new_retry_queue)
    declared_new_dead_queue = await broker.declare_queue(new_dead_queue)
    declared_webhook_queue = await broker.declare_queue(webhook_queue)
    declared_webhook_retry_queue = await broker.declare_queue(webhook_retry_queue)
    declared_webhook_dead_queue = await broker.declare_queue(dead_queue)

    await declared_new_queue.bind(declared_payments_exchange, routing_key=topics.NEW)
    await declared_new_retry_queue.bind(declared_retry_exchange, routing_key=topics.NEW_RETRY)
    await declared_new_dead_queue.bind(declared_dead_exchange, routing_key=topics.NEW_DEAD)
    await declared_webhook_queue.bind(declared_payments_exchange, routing_key=topics.WEBHOOKS)
    await declared_webhook_retry_queue.bind(declared_retry_exchange, routing_key=topics.WEBHOOKS_RETRY)
    await declared_webhook_dead_queue.bind(declared_dead_exchange, routing_key=topics.WEBHOOKS_DEAD)
