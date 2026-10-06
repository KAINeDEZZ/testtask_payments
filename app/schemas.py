import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_validator

from app.models import PaymentStatus
from app.webhook_security import validate_webhook_host


class PaymentCreate(BaseModel):
    amount: Decimal = Field(gt=0, max_digits=14, decimal_places=2)
    currency: Literal["RUB", "USD", "EUR"]
    description: str | None = Field(default=None, max_length=10_000)
    metadata: dict[str, Any] | None = None
    webhook_url: AnyHttpUrl

    @field_validator("webhook_url")
    @classmethod
    def _reject_internal_hosts(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        validate_webhook_host(str(value))
        return value


class PaymentAccepted(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentRead(PaymentAccepted):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    amount: Decimal
    currency: str
    description: str | None
    metadata: dict[str, Any] | None
    webhook_url: str
    processed_at: datetime | None

