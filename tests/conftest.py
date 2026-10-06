import os

os.environ.setdefault("API_KEY", "test-api-key-123456")
# Docker Desktop on macOS exposes a per-user socket that the Ryuk reaper cannot mount.
os.environ.setdefault("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "/var/run/docker.sock")

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.community.postgres import PostgresContainer

from app.db import get_session
from app.main import app
from app.models import Base

API_KEY = os.environ["API_KEY"]


@pytest.fixture(scope="session")
def database_url():
    # A disposable PostgreSQL per test session: no compose stack or local database needed.
    with PostgresContainer("postgres:16-alpine", driver="asyncpg") as postgres:
        yield postgres.get_connection_url()


@pytest.fixture
async def session_factory(database_url):
    engine = create_async_engine(database_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@pytest.fixture
async def client(session_factory):
    async def override():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()
