FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY pyproject.toml ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
        "aiosqlite>=0.20.0" \
        "alembic>=1.13.0" \
        "asyncpg>=0.29.0" \
        "fastapi>=0.115.0" \
        "faststream[rabbit]>=0.5.0" \
        "httpx>=0.27.0" \
        "pydantic>=2.9.0" \
        "pydantic-settings>=2.5.0" \
        "sqlalchemy[asyncio]>=2.0.0" \
        "uvicorn[standard]>=0.30.0"

COPY alembic.ini ./
COPY alembic ./alembic
COPY app ./app

