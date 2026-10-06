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


@lru_cache
def get_settings() -> Settings:
    return Settings()

