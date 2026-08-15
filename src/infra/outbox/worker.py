import logging

from faststream.rabbit import RabbitBroker

from src.infra.outbox.interfaces import OutboxUnitOfWork

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)


class OutboxWorker:
    def __init__(self, uow: OutboxUnitOfWork, broker: RabbitBroker):
        self._uow = uow
        self._broker = broker

    async def process_message(self) -> None:
        async with self._uow as uow:
            messages = await uow.outbox.get_unpublished()
            if not messages:
                await uow.commit()
                return

            published_count = 0
            try:
                for msg in messages:
                    await self._broker.publish(message=msg.body, queue=msg.topic)
                    await uow.outbox.mark_as_published(msg)
                    published_count += 1
                await uow.commit()
            except Exception:
                logger.exception(
                    "Error publishing outbox events (published=%d/%d). Rollback keeps unpublished for retry.",
                    published_count,
                    len(messages),
                )
                raise
