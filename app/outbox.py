"""Transactional-outbox publishing for payment events."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.messaging import publish_payment_event
from app.models import Outbox, OutboxStatus

logger = logging.getLogger(__name__)

PENDING = OutboxStatus.pending
PUBLISHED = OutboxStatus.published


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _status_value(current: Any, value: str) -> Any:
    """Preserve an Enum-backed ORM column while supporting plain strings."""
    status_type = type(current)
    if hasattr(status_type, "__members__"):
        try:
            return status_type(value)
        except ValueError:
            return status_type[value.upper()]
    return value


class OutboxPublisher:
    """Safely drains rows written in the same transaction as a payment.

    Rows are locked while publishing so multiple API/worker instances do not
    produce duplicate messages under normal operation.  Delivery remains
    at-least-once: a process crash after broker confirmation and before commit
    can republish the event, so consumers must remain idempotent.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        batch_size: int | None = None,
        poll_interval: float | None = None,
        publish: Callable[[dict[str, Any]], Any] = publish_payment_event,
    ) -> None:
        self._session_factory = session_factory
        self._batch_size = batch_size or int(os.getenv("OUTBOX_BATCH_SIZE", "100"))
        self._poll_interval = poll_interval or float(os.getenv("OUTBOX_POLL_INTERVAL", "1"))
        self._publish = publish
        self._max_retries = max(1, int(os.getenv("OUTBOX_MAX_RETRIES", "10")))
        self._stop = asyncio.Event()

    async def publish_pending(self) -> int:
        """Publish one locked batch and return the number of sent messages."""
        sent = 0
        async with self._session_factory() as session:
            result = await session.execute(
                select(Outbox)
                .where(Outbox.status == PENDING)
                .order_by(Outbox.created_at)
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )
            events = list(result.scalars())
            for event in events:
                try:
                    await self._publish(event.payload)
                except Exception:
                    event.retry_count = (event.retry_count or 0) + 1
                    logger.exception("Unable to publish outbox event %s", event.id)
                    if event.retry_count >= self._max_retries:
                        # Park a poison event instead of retrying it forever.
                        event.status = OutboxStatus.failed
                        logger.error("Outbox event %s marked failed after %s attempts", event.id, event.retry_count)
                    continue
                event.status = _status_value(event.status, PUBLISHED)
                event.published_at = _utcnow()
                sent += 1
            await session.commit()
        return sent

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
