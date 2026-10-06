from typing import Annotated

from fastapi import Header, HTTPException, status
from secrets import compare_digest

from app.config import get_settings


async def require_api_key(
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> None:
    configured_key = get_settings().api_key.get_secret_value()
    if not x_api_key or not compare_digest(x_api_key, configured_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key",
        )
