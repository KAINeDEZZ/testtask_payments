"""FastStream worker consuming newly-created payments."""

from __future__ import annotations

import asyncio
import logging
import os
import random
import signal
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx
from faststream.exceptions import NackMessage
from faststream.rabbit.annotations import RabbitMessage
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.messaging import (
    PAYMENTS_QUEUE_NAME,
    broker,
    dead_letter_exchange,
    payments_dead_letter_queue,
    payments_exchange,
    payments_queue,
)
from app.db import SessionLocal
from app.models import Payment, PaymentStatus
from app.outbox import OutboxPublisher
from app.webhook_security import resolves_to_public_address

logger = logging.getLogger(__name__)

ATTEMPT_HEADER = "x-attempt"
FINAL_STATUSES = {"succeeded", "failed"}


def _max_processing_attempts() -> int:
    """Total consumer attempts before an event is dead-lettered (three by default)."""
    try:
        return max(1, int(os.getenv("PAYMENT_MAX_ATTEMPTS", "3")))
    except ValueError:
        return 3


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _success_rate() -> float:
    try:
        return min(1.0, max(0.0, float(os.getenv("PAYMENT_SUCCESS_RATE", "0.9"))))
    except ValueError:
        logger.warning("PAYMENT_SUCCESS_RATE must be a number; using 0.9")
        return 0.9


def _webhook_retries() -> int:
    """Number of retries after the initial webhook attempt (three by default)."""
    try:
        configured = os.getenv("WEBHOOK_MAX_RETRIES", os.getenv("WEBHOOK_MAX_ATTEMPTS", "3"))
        return max(0, int(configured))
    except ValueError:
        return 3


async def deliver_webhook(url: str, body: dict[str, Any]) -> bool:
    """POST the final state, retrying transient errors with exponential backoff."""
    if not await resolves_to_public_address(url):
        logger.warning("Webhook %s resolves to a non-public address; not delivering", url)
        return False
    retries = _webhook_retries()
    attempts = retries + 1
    timeout = float(os.getenv("WEBHOOK_TIMEOUT_SECONDS", "10"))
    async with httpx.AsyncClient(timeout=timeout) as client:
        for attempt in range(attempts):
            try:
                response = await client.post(url, json=body)
                response.raise_for_status()
                return True
            except (httpx.HTTPError, ValueError) as exc:
                if attempt == attempts - 1:
                    logger.warning("Webhook delivery failed after %s attempts: %s", attempts, exc)
                    return False
                await asyncio.sleep(2**attempt)
    return False


class PaymentWorker:
    """Payment processor with injectable database access for tests and runtime."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def process(self, message: dict[str, Any]) -> None:
        payment_id = message.get("payment_id") or message.get("id")
        if not payment_id:
            raise ValueError("payment event has no payment_id")
        payment_id = uuid.UUID(str(payment_id))

        async with self._session_factory() as session:
            payment = await session.scalar(select(Payment).where(Payment.id == payment_id))
            if payment is None:
                logger.warning("Payment %s from queue was not found", payment_id)
                return
            already_final = str(getattr(payment.status, "value", payment.status)) in FINAL_STATUSES

        if not already_final:
            # The requested simulation has an observable 2–5-second asynchronous
            # delay and can be made deterministic in integration tests with env vars.
            await asyncio.sleep(random.uniform(2, 5))
            status = "succeeded" if random.random() < _success_rate() else "failed"
            async with self._session_factory() as session:
                # Conditional UPDATE: only one concurrent consumer can move the
                # payment out of "pending"; a redelivery never overwrites the result.
                await session.execute(
                    update(Payment)
                    .where(Payment.id == payment_id, Payment.status == PaymentStatus.pending)
                    .values(status=PaymentStatus(status), processed_at=_utcnow())
                )
                await session.commit()

        async with self._session_factory() as session:
            payment = await session.scalar(select(Payment).where(Payment.id == payment_id))
            if payment is None or payment.webhook_sent_at is not None or not payment.webhook_url:
                return
            webhook_url = payment.webhook_url
            webhook_body = {
                "payment_id": str(payment.id),
                "status": str(getattr(payment.status, "value", payment.status)),
                "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
            }

        # The webhook is tracked separately from the status, so a crash after the
        # status commit leads to a redelivery that still notifies the client.
        if not await deliver_webhook(webhook_url, webhook_body):
            raise RuntimeError(f"webhook delivery failed for payment {payment_id}")

        async with self._session_factory() as session:
            await session.execute(
                update(Payment).where(Payment.id == payment_id).values(webhook_sent_at=_utcnow())
            )
            await session.commit()


def register_payment_consumer(session_factory: async_sessionmaker[AsyncSession]) -> PaymentWorker:
    """Attach the durable queue and DLQ topology to the shared FastStream broker."""
    worker = PaymentWorker(session_factory)

    # Declaring a subscriber on the DLQ makes its binding explicit; invalid
    # events are logged for operations and are not automatically reprocessed.
    @broker.subscriber(payments_dead_letter_queue, exchange=dead_letter_exchange)
    async def dead_letter(message: dict[str, Any]) -> None:
        logger.error("Payment event sent to DLQ: %s", message)

    @broker.subscriber(payments_queue, exchange=payments_exchange)
    async def consume(message: dict[str, Any], msg: RabbitMessage) -> None:
        attempt = int((msg.headers or {}).get(ATTEMPT_HEADER, 1))
        try:
            await worker.process(message)
        except Exception:
            max_attempts = _max_processing_attempts()
            if attempt >= max_attempts:
                logger.exception("Payment event failed %s attempts; sending it to the DLQ", attempt)
                raise NackMessage(requeue=False)
            logger.exception("Payment event attempt %s/%s failed; retrying", attempt, max_attempts)
            await asyncio.sleep(2 ** (attempt - 1))
            # Republish with an incremented counter, then ack the original:
            # plain requeue would lose track of how many attempts were made.
            await broker.publish(
                message,
                exchange=payments_exchange,
                routing_key=PAYMENTS_QUEUE_NAME,
                headers={ATTEMPT_HEADER: attempt + 1},
                persist=True,
            )

    return worker


async def run_worker() -> None:
    """Run the broker consumer and transactional-outbox publisher until signalled."""
    register_payment_consumer(SessionLocal)
    publisher = OutboxPublisher(SessionLocal)
    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop_requested.set)
        except NotImplementedError:  # Windows event loops do not support it.
            pass

    await broker.start()
    publisher_task = asyncio.create_task(publisher.run(), name="outbox-publisher")
    try:
        await stop_requested.wait()
    finally:
        publisher.stop()
        publisher_task.cancel()
        try:
            await publisher_task
        except asyncio.CancelledError:
            pass
        await broker.close()


if __name__ == "__main__":
    asyncio.run(run_worker())
