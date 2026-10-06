from sqlalchemy import func, select

from app.models import Outbox
from tests.conftest import API_KEY

BODY = {"amount": "100.00", "currency": "RUB", "webhook_url": "https://example.com/hook"}


def headers(key="k1"):
    return {"X-API-Key": API_KEY, "Idempotency-Key": key}


async def test_create_payment_writes_outbox(client, session_factory):
    resp = await client.post("/api/v1/payments", json=BODY, headers=headers())
    assert resp.status_code == 202
    assert resp.json()["status"] == "pending"
    async with session_factory() as s:
        assert await s.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_same_key_same_body_returns_original(client, session_factory):
    first = await client.post("/api/v1/payments", json=BODY, headers=headers())
    second = await client.post("/api/v1/payments", json=BODY, headers=headers())
    assert second.status_code == 202
    assert first.json()["payment_id"] == second.json()["payment_id"]
    async with session_factory() as s:
        assert await s.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_same_key_different_body_conflicts(client):
    await client.post("/api/v1/payments", json=BODY, headers=headers())
    resp = await client.post("/api/v1/payments", json={**BODY, "amount": "200.00"}, headers=headers())
    assert resp.status_code == 409


async def test_get_payment(client):
    created = (await client.post("/api/v1/payments", json=BODY, headers=headers())).json()
    resp = await client.get(f"/api/v1/payments/{created['payment_id']}", headers={"X-API-Key": API_KEY})
    assert resp.status_code == 200
    assert resp.json()["amount"] == "100.00"


async def test_requires_idempotency_key(client):
    resp = await client.post("/api/v1/payments", json=BODY, headers={"X-API-Key": API_KEY})
    assert resp.status_code == 422


async def test_rejects_wrong_api_key(client):
    resp = await client.post("/api/v1/payments", json=BODY, headers={"X-API-Key": "wrong", "Idempotency-Key": "k"})
    assert resp.status_code in (401, 403)


async def test_rejects_internal_webhook(client):
    for url in ("http://localhost:8000/x", "http://127.0.0.1/x", "http://10.0.0.1/x"):
        resp = await client.post("/api/v1/payments", json={**BODY, "webhook_url": url}, headers=headers(url))
        assert resp.status_code == 422, url
