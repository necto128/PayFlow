import pytest

from src.app.dto.payments import PaymentUpdateStatusRequestDTO
from src.domains.payments.domain.constraints import PaymentStatusEnum


@pytest.mark.asyncio
async def test_should_change_pending_payment_status(
    payment_factory, payments_repository, outbox_repository, payments_uow, change_payment_status_use_case
):
    payment = payment_factory(webhook_url=None)
    payments_repository.get.return_value = payment

    dto = PaymentUpdateStatusRequestDTO(id=payment.id, status=PaymentStatusEnum.SUCCESS)

    result = await change_payment_status_use_case.execute(dto)

    assert result is payment
    assert payment.status.value == PaymentStatusEnum.SUCCESS
    assert payment.processed_at is not None

    payments_repository.get.assert_awaited_once_with(entity_id=payment.id, for_update=True)
    payments_repository.update.assert_awaited_once_with(payment)
    outbox_repository.add_msgs.assert_not_awaited()
    payments_uow.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_should_enqueue_webhook_when_url_present(
    payment_factory, payments_repository, outbox_repository, payments_uow, change_payment_status_use_case
):
    payment = payment_factory(webhook_url="https://example.com/hook")
    payments_repository.get.return_value = payment

    dto = PaymentUpdateStatusRequestDTO(id=payment.id, status=PaymentStatusEnum.SUCCESS)

    await change_payment_status_use_case.execute(dto)

    outbox_repository.add_msgs.assert_awaited_once()
    payments_uow.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_should_not_change_processed_payment(
    payment_factory, payments_repository, payments_uow, change_payment_status_use_case
):
    payment = payment_factory()
    payment.set_status(PaymentStatusEnum.SUCCESS)
    payments_repository.get.return_value = payment
    dto = PaymentUpdateStatusRequestDTO(id=payment.id, status=PaymentStatusEnum.FAILED)

    result = await change_payment_status_use_case.execute(dto)

    assert result is None
    assert payment.status.value == PaymentStatusEnum.SUCCESS

    payments_repository.update.assert_not_awaited()
    payments_uow.commit.assert_not_awaited()
