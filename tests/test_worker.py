import uuid

import httpx
import pytest
from sqlalchemy import select

import app.worker as worker_mod
from app.models import Outbox, OutboxStatus, Payment, PaymentStatus
from app.outbox import OutboxPublisher
from app.worker import PaymentWorker, deliver_webhook


@pytest.fixture(autouse=True)
def no_delays(monkeypatch):
    monkeypatch.setattr(worker_mod.random, "uniform", lambda a, b: 0)

    async def no_sleep(_):
        return None

    monkeypatch.setattr(worker_mod.asyncio, "sleep", no_sleep)


async def make_payment(session_factory) -> uuid.UUID:
    async with session_factory() as s:
        p = Payment(amount=10, currency="RUB", idempotency_key=str(uuid.uuid4()), webhook_url="https://example.com/h")
        s.add(p)
        await s.commit()
        return p.id


async def load(session_factory, pid) -> Payment:
    async with session_factory() as s:
        return await s.scalar(select(Payment).where(Payment.id == pid))


async def test_process_sets_final_status_and_sends_webhook_once(session_factory, monkeypatch):
    sent = []

    async def fake_deliver(url, body):
        sent.append(body)
        return True

    monkeypatch.setattr(worker_mod, "deliver_webhook", fake_deliver)
    pid = await make_payment(session_factory)
    worker = PaymentWorker(session_factory)

    await worker.process({"payment_id": str(pid)})
    first = await load(session_factory, pid)
    assert first.status in (PaymentStatus.succeeded, PaymentStatus.failed)
    assert first.webhook_sent_at is not None

    # Redelivery must not change the result or notify again.
    await worker.process({"payment_id": str(pid)})
    second = await load(session_factory, pid)
    assert second.status == first.status
    assert len(sent) == 1
    assert sent[0]["status"] == first.status.value


async def test_failed_webhook_raises_and_is_retried_later(session_factory, monkeypatch):
    results = iter([False, True])

    async def flaky_deliver(url, body):
        return next(results)

    monkeypatch.setattr(worker_mod, "deliver_webhook", flaky_deliver)
    pid = await make_payment(session_factory)
    worker = PaymentWorker(session_factory)

    with pytest.raises(RuntimeError):
        await worker.process({"payment_id": str(pid)})
    assert (await load(session_factory, pid)).webhook_sent_at is None

    await worker.process({"payment_id": str(pid)})
    assert (await load(session_factory, pid)).webhook_sent_at is not None


async def test_deliver_webhook_retries_then_gives_up(monkeypatch):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(500)

    real_client = httpx.AsyncClient

    async def public(_):
        return True

    monkeypatch.setattr(worker_mod, "resolves_to_public_address", public)
    monkeypatch.setattr(worker_mod.httpx, "AsyncClient", lambda **kw: real_client(transport=httpx.MockTransport(handler), **kw))
    monkeypatch.setenv("WEBHOOK_MAX_ATTEMPTS", "3")

    assert await deliver_webhook("https://example.com/h", {}) is False
    assert len(calls) == 4  # initial attempt + 3 retries


async def test_outbox_marks_poison_event_failed(session_factory, monkeypatch):
    monkeypatch.setenv("OUTBOX_MAX_RETRIES", "2")
    pid = await make_payment(session_factory)
    async with session_factory() as s:
        s.add(Outbox(event_type="payments.new", aggregate_id=pid, payload={"payment_id": str(pid)}))
        await s.commit()

    async def broken_publish(_):
        raise ConnectionError("broker down")

    publisher = OutboxPublisher(session_factory, publish=broken_publish)
    await publisher.publish_pending()
    await publisher.publish_pending()
    await publisher.publish_pending()  # already failed: must not be retried

    async with session_factory() as s:
        event = await s.scalar(select(Outbox))
    assert event.status == OutboxStatus.failed
    assert event.retry_count == 2
