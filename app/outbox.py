"""Transactional-outbox publishing for payment events."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Callable
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.messaging import publish_payment_event
from app.models import Outbox, OutboxStatus

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class OutboxPublisher:
    """Safely drains rows written in the same transaction as a payment.

    A batch is claimed with a short lease (``locked_until``) in its own
    transaction, then published without holding a database connection open.
    Concurrent publishers skip leased rows; an expired lease (crashed
    publisher) makes the row eligible again. Delivery is at-least-once, so
    consumers must remain idempotent.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        batch_size: int | None = None,
        poll_interval: float | None = None,
        publish: Callable[[dict[str, Any]], Any] = publish_payment_event,
    ) -> None:
        settings = get_settings()
        self._session_factory = session_factory
        self._batch_size = batch_size or settings.outbox_batch_size
        self._poll_interval = poll_interval or settings.outbox_poll_interval
        self._max_retries = settings.outbox_max_retries
        self._lease = timedelta(seconds=settings.outbox_lock_seconds)
        self._publish = publish
        self._stop = asyncio.Event()

    async def publish_pending(self) -> int:
        """Publish one claimed batch and return the number of sent messages."""
        sent = 0
        for event_id, payload in await self._claim_batch():
            try:
                await self._publish(payload)
            except Exception:
                logger.exception("Unable to publish outbox event %s", event_id)
                await self._record_failure(event_id)
                continue
            await self._finish(event_id, status=OutboxStatus.published, published_at=_utcnow())
            sent += 1
        return sent

    async def _claim_batch(self) -> list[tuple[uuid.UUID, dict[str, Any]]]:
        now = _utcnow()
        async with self._session_factory() as session:
            result = await session.execute(
                select(Outbox)
                .where(
                    Outbox.status == OutboxStatus.pending,
                    or_(Outbox.locked_until.is_(None), Outbox.locked_until < now),
                )
                .order_by(Outbox.created_at)
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )
            events = list(result.scalars())
            for event in events:
                event.locked_until = now + self._lease
            claimed = [(event.id, event.payload) for event in events]
            await session.commit()
        return claimed

    async def _record_failure(self, event_id: uuid.UUID) -> None:
        async with self._session_factory() as session:
            event = await session.get(Outbox, event_id)
            if event is None:
                return
            event.retry_count = (event.retry_count or 0) + 1
            event.locked_until = None
            if event.retry_count >= self._max_retries:
                # Park a poison event instead of retrying it forever.
                event.status = OutboxStatus.failed
                logger.error("Outbox event %s marked failed after %s attempts", event_id, event.retry_count)
            await session.commit()

    async def _finish(self, event_id: uuid.UUID, **values: Any) -> None:
        async with self._session_factory() as session:
            await session.execute(update(Outbox).where(Outbox.id == event_id).values(locked_until=None, **values))
            await session.commit()

    async def run(self) -> None:
        """Poll until ``stop`` is requested; suitable for a worker lifespan."""
        while not self._stop.is_set():
            try:
                await self.publish_pending()
            except Exception:
                logger.exception("Outbox polling iteration failed")
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=self._poll_interval)

    def stop(self) -> None:
        self._stop.set()
