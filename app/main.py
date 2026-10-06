import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import engine, get_session
from app.dependencies import require_api_key
from app.models import Base, Outbox, OutboxStatus, Payment, PaymentStatus
from app.schemas import PaymentAccepted, PaymentCreate, PaymentRead


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    # Production deployments use Alembic. Creating tables here keeps the service
    # convenient to run in an empty local development database.
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


app = FastAPI(title="Payments service", version="1.0.0", lifespan=lifespan)


def accepted_response(payment: Payment) -> PaymentAccepted:
    return PaymentAccepted(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


def payment_response(payment: Payment) -> PaymentRead:
    return PaymentRead(
        id=payment.id,
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_,
        status=payment.status,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )


def _replay(existing: Payment, payload: PaymentCreate) -> PaymentAccepted:
    """Return the original operation, refusing to reuse a key for a different request."""
    same_request = (
        existing.amount == payload.amount
        and existing.currency == payload.currency
        and existing.description == payload.description
        and existing.metadata_ == payload.metadata
        and existing.webhook_url == str(payload.webhook_url)
    )
    if not same_request:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request body",
        )
    return accepted_response(existing)


@app.post(
    "/api/v1/payments",
    response_model=PaymentAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_api_key)],
)
async def create_payment(
    payload: PaymentCreate,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    session: AsyncSession = Depends(get_session),
) -> PaymentAccepted:
    if idempotency_key is None or not idempotency_key.strip():
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Idempotency-Key is required")
    if len(idempotency_key) > 255:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Idempotency-Key is too long")

    key = idempotency_key.strip()
    existing = await session.scalar(select(Payment).where(Payment.idempotency_key == key))
    if existing is not None:
        return _replay(existing, payload)

    payment = Payment(
        amount=payload.amount,
        currency=payload.currency,
        description=payload.description,
        metadata_=payload.metadata,
        status=PaymentStatus.pending,
        idempotency_key=key,
        webhook_url=str(payload.webhook_url),
    )
    session.add(payment)
    try:
        await session.flush()
        session.add(
            Outbox(
                event_type="payments.new",
                aggregate_id=payment.id,
                payload={"payment_id": str(payment.id)},
                status=OutboxStatus.pending,
            )
        )
        await session.commit()
    except IntegrityError:
        # A competing request may have created the same idempotency key after
        # the initial lookup. Return the persisted operation rather than 500.
        await session.rollback()
        existing = await session.scalar(select(Payment).where(Payment.idempotency_key == key))
        if existing is not None:
            return _replay(existing, payload)
        raise

    return accepted_response(payment)


@app.get(
    "/api/v1/payments/{payment_id}",
    response_model=PaymentRead,
    dependencies=[Depends(require_api_key)],
)
async def get_payment(
    payment_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
) -> PaymentRead:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return payment_response(payment)


@app.get("/health", include_in_schema=False)
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}
