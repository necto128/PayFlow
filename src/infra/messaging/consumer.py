import asyncio
import logging
import random

from aiohttp import ClientSession
from dishka_faststream import FromDishka
from faststream import AckPolicy, Context
from faststream.rabbit import RabbitBroker, RabbitMessage, RabbitRouter

from src.app.dto.payments import PaymentUpdateStatusRequestDTO
from src.app.use_cases.change_payment_status import ChangePaymentStatusUseCase
from src.core import settings
from src.domains.payments.domain.constraints import PaymentStatusEnum
from src.infra.adapters.brokers.topology import (
    dead_exchange,
    get_death_count,
    payments_exchange,
    webhook_queue,
)
from src.infra.messaging.events.emitted import PaymentCreatedEvent
from src.infra.messaging.events.emitted.events import PaymentProcessedEvent

logging.basicConfig(level=settings.LOGGING.LEVEL, format=settings.LOGGING.FORMAT)
logger = logging.getLogger(__name__)
router = RabbitRouter()

topics = settings.MESSAGING.PAYMENTS.TOPICS


async def emulate_payment_processing() -> bool:
    processing_delay_seconds = random.uniform(0.2, 1.0)
    await asyncio.sleep(processing_delay_seconds)
    return random.random() < 0.9


@router.subscriber(queue=topics.NEW, exchange=payments_exchange, ack_policy=AckPolicy.MANUAL)
async def process_payment(
    event: PaymentCreatedEvent,
    change_payment_status: FromDishka[ChangePaymentStatusUseCase],
    message: RabbitMessage,
    broker: RabbitBroker = Context(),
) -> None:
    payment_id = str(event.id)
    retry_count = get_death_count(message.headers)

    if retry_count >= settings.MESSAGING.MAX_RETRIES:
        logger.error(
            "Payment %s exceeded max retries (%d). Moving to DLQ.",
            payment_id,
            settings.MESSAGING.MAX_RETRIES,
        )
        await broker.publish(
            message=event.as_dict(),
            exchange=dead_exchange,
            routing_key=topics.NEW_DEAD,
        )
        await message.ack()
        return

    logger.info("Processing payment %s (retry=%d)", payment_id, retry_count)

    try:
        gateway_accepted = await emulate_payment_processing()
        payment_status = PaymentStatusEnum.SUCCESS if gateway_accepted else PaymentStatusEnum.FAILED
        payment_update = PaymentUpdateStatusRequestDTO(id=event.id, status=payment_status)
        await change_payment_status.execute(payment=payment_update, webhook_url=event.webhook_url)
        logger.info("Payment %s → %s", payment_id, payment_status)
        await message.ack()
    except Exception:
        logger.exception("Unexpected error processing payment %s", payment_id)
        await message.nack(requeue=False)


@router.subscriber(queue=webhook_queue, exchange=payments_exchange, ack_policy=AckPolicy.MANUAL)
async def send_webhook_consumer(
    event: PaymentProcessedEvent,
    http_client: FromDishka[ClientSession],
    message: RabbitMessage,
    broker: RabbitBroker = Context(),
) -> None:
    payment_id = str(event.id)
    retry_count = get_death_count(message.headers)

    if retry_count >= settings.MESSAGING.MAX_RETRIES:
        logger.error(
            "Webhook for payment %s exceeded max retries (%d). Moving to DLQ.",
            payment_id,
            settings.MESSAGING.MAX_RETRIES,
        )
        await broker.publish(
            message=event.as_dict(),
            exchange=dead_exchange,
            routing_key=topics.WEBHOOKS_DEAD,
        )
        await message.ack()
        return

    if not event.webhook_url:
        await message.ack()
        return

    webhook_payload = {"payment_id": payment_id, "status": event.status}
    webhook_headers = {"X-Event-Id": str(event.event_id)}
    webhook_url = event.webhook_url.unicode_string()

    try:
        async with http_client.post(
            url=webhook_url, json=webhook_payload, headers=webhook_headers, timeout=10
        ) as response:
            response.raise_for_status()
        logger.info("Webhook sent for payment %s (retry=%d)", payment_id, retry_count)
        await message.ack()
    except Exception as exc:
        logger.warning(
            "Webhook failed for payment %s (retry=%d): %s. Sending to retry queue via DLX.",
            payment_id,
            retry_count,
            exc,
        )
        await message.nack(requeue=False)
