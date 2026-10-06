"""RabbitMQ topology and message helpers for payment events."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from app.config import get_settings
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue


PAYMENTS_EXCHANGE_NAME = "payments"
PAYMENTS_QUEUE_NAME = "payments.new"
PAYMENTS_DLQ_NAME = "payments.new.dlq"
PAYMENTS_DLX_NAME = "payments.dlx"


def _broker_url() -> str:
    """Return a single, easily configurable RabbitMQ connection URL."""
    return get_settings().rabbitmq_url


payments_exchange = RabbitExchange(
    PAYMENTS_EXCHANGE_NAME,
    type=ExchangeType.DIRECT,
    durable=True,
)
dead_letter_exchange = RabbitExchange(
    PAYMENTS_DLX_NAME,
    type=ExchangeType.DIRECT,
    durable=True,
)
payments_queue = RabbitQueue(
    PAYMENTS_QUEUE_NAME,
    durable=True,
    arguments={
        "x-dead-letter-exchange": PAYMENTS_DLX_NAME,
        "x-dead-letter-routing-key": PAYMENTS_QUEUE_NAME,
    },
)
payments_dead_letter_queue = RabbitQueue(
    PAYMENTS_DLQ_NAME,
    durable=True,
    routing_key=PAYMENTS_QUEUE_NAME,
)

# The worker imports this object and attaches subscribers to it.  The API can
# use the same topology through ``publish_payment_event`` without importing a
# worker process.
broker = RabbitBroker(_broker_url())


def json_default(value: Any) -> str:
    """Serialize common ORM payload values without losing useful information."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (UUID, Decimal, Enum)):
        return str(value.value if isinstance(value, Enum) else value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def normalize_payload(payload: Any) -> dict[str, Any]:
    """Turn a JSON ORM value or a JSON string into an isolated dictionary."""
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, dict):
        raise ValueError("Outbox payload must be a JSON object")
    # Round trip both detaches SQLAlchemy mutable JSON values and guarantees the
    # data can cross the message boundary.
    return json.loads(json.dumps(payload, default=json_default))


async def publish_payment_event(payload: dict[str, Any]) -> None:
    """Publish one payment-created event to the durable payment queue."""
    await broker.publish(
        normalize_payload(payload),
        exchange=payments_exchange,
        routing_key=PAYMENTS_QUEUE_NAME,
    )
