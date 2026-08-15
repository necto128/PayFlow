from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import aiohttp
import pytest

from src.infra.messaging.consumer import send_webhook_consumer
from src.infra.messaging.events.emitted.events import PaymentProcessedEvent


@pytest.mark.asyncio
async def test_should_ack_when_webhook_succeeds():
    event = PaymentProcessedEvent(
        id=uuid4(),
        event_id=uuid4(),
        status="SUCCESS",
        webhook_url="https://example.com",
    )

    message = AsyncMock()
    message.headers = {}
    broker = AsyncMock()

    response = AsyncMock()
    response.raise_for_status = MagicMock()

    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = response
    context_manager.__aexit__.return_value = None

    http_client = MagicMock()
    http_client.post.return_value = context_manager

    await send_webhook_consumer(event=event, http_client=http_client, message=message, broker=broker)

    http_client.post.assert_called_once()
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()
    broker.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_should_nack_to_retry_when_webhook_fails():
    event = PaymentProcessedEvent(
        id=uuid4(),
        event_id=uuid4(),
        status="SUCCESS",
        webhook_url="https://example.com",
    )

    message = AsyncMock()
    message.headers = {}
    broker = AsyncMock()

    http_client = MagicMock()
    http_client.post.side_effect = aiohttp.ClientError()

    await send_webhook_consumer(event=event, http_client=http_client, message=message, broker=broker)

    message.nack.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()
    broker.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_should_move_to_dlq_when_retries_exhausted():
    event = PaymentProcessedEvent(
        id=uuid4(),
        event_id=uuid4(),
        status="SUCCESS",
        webhook_url="https://example.com",
    )

    message = AsyncMock()
    message.headers = {"x-death": [{"count": 3, "queue": "payments.webhooks"}]}
    broker = AsyncMock()
    http_client = MagicMock()

    await send_webhook_consumer(event=event, http_client=http_client, message=message, broker=broker)

    broker.publish.assert_awaited_once()
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()
    http_client.post.assert_not_called()
