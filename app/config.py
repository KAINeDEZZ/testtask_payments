from functools import lru_cache

from pydantic import Field, PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables or a .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = Field(
        default="postgresql+asyncpg://payments:payments@localhost:5432/payments",
        validation_alias="DATABASE_URL",
    )
    # No code default: the service refuses to start without an explicit key.
    api_key: SecretStr = Field(validation_alias="API_KEY", min_length=16)
    sql_echo: bool = Field(default=False, validation_alias="SQL_ECHO")
    rabbitmq_url: str = Field(default="amqp://guest:guest@rabbitmq:5672/", validation_alias="RABBITMQ_URL")

    payment_success_rate: float = Field(default=0.9, ge=0, le=1, validation_alias="PAYMENT_SUCCESS_RATE")
    payment_max_attempts: int = Field(default=3, ge=1, validation_alias="PAYMENT_MAX_ATTEMPTS")

    # Retries after the initial webhook attempt.
    webhook_max_attempts: int = Field(default=3, ge=0, validation_alias="WEBHOOK_MAX_ATTEMPTS")
    webhook_timeout_seconds: float = Field(default=10, gt=0, validation_alias="WEBHOOK_TIMEOUT_SECONDS")
    allow_private_webhooks: bool = Field(default=False, validation_alias="ALLOW_PRIVATE_WEBHOOKS")

    outbox_batch_size: int = Field(default=100, ge=1, validation_alias="OUTBOX_BATCH_SIZE")
    outbox_poll_interval: float = Field(default=1, gt=0, validation_alias="OUTBOX_POLL_INTERVAL")
    outbox_max_retries: int = Field(default=10, ge=1, validation_alias="OUTBOX_MAX_RETRIES")
    outbox_lock_seconds: int = Field(default=30, ge=1, validation_alias="OUTBOX_LOCK_SECONDS")


@lru_cache
def get_settings() -> Settings:
    return Settings()

