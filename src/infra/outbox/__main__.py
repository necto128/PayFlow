import asyncio
import logging

from faststream.rabbit import RabbitBroker

from src.containers import container
from src.core import settings
from src.infra.adapters.brokers.topology import setup_rabbit_infrastructure
from src.infra.outbox.worker import OutboxWorker

logging.basicConfig(level=settings.LOGGING.LEVEL, format=settings.LOGGING.FORMAT)
logger = logging.getLogger(__name__)


async def run_worker():
    async with container() as ioc:
        worker = await ioc.get(OutboxWorker)
        broker = await ioc.get(RabbitBroker)
        try:
            await broker.start()
            await setup_rabbit_infrastructure(broker)
            while True:
                await worker.process_message()
                await asyncio.sleep(1)
        finally:
            await broker.stop()
            logger.info("Outbox worker stopped")


async def main():
    logger.info("Launch outbox worker")
    await run_worker()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
