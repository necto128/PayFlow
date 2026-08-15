from dataclasses import dataclass
from typing import Optional

from pydantic import AnyUrl

from src.app.dto.payments import PaymentUpdateStatusRequestDTO
from src.app.interfaces.units_of_work import ApplicationPaymentUnitOfWork
from src.core import settings
from src.domains.payments.domain.constraints import PaymentStatusEnum
from src.domains.payments.domain.entities import PaymentEntity
from src.infra.messaging.events.emitted.events import PaymentProcessedEvent
from src.infra.outbox.schemes import OutboxMessageScheme


@dataclass(frozen=True, slots=True)
class ChangePaymentStatusUseCase:
    uow: ApplicationPaymentUnitOfWork

    async def execute(
        self,
        payment: PaymentUpdateStatusRequestDTO,
        webhook_url: Optional[AnyUrl] = None,
    ) -> Optional[PaymentEntity]:
        async with self.uow as uow:
            entity = await uow.payments.get(entity_id=payment.id, for_update=True)

            if entity.status.as_plain() != PaymentStatusEnum.PENDING:
                return None

            entity.set_status(payment.status)
            entity.process()
            await uow.payments.update(entity)

            effective_webhook_url = webhook_url or entity.webhook_url.value
            if effective_webhook_url:
                outbox_message = OutboxMessageScheme(
                    body=PaymentProcessedEvent(
                        id=entity.id,
                        status=payment.status,
                        webhook_url=effective_webhook_url,
                    ).as_dict(),
                    topic=settings.MESSAGING.PAYMENTS.TOPICS.WEBHOOKS,
                )
                await uow.outbox.add_msgs(outbox_message)

            await uow.commit()
            return entity
