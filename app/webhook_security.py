"""SSRF guard for client-supplied webhook URLs."""

from __future__ import annotations

import asyncio
import ipaddress
from urllib.parse import urlsplit

from app.config import get_settings


def private_webhooks_allowed() -> bool:
    """Local development may opt in to webhooks on private/loopback hosts."""
    return get_settings().allow_private_webhooks


def _is_public_ip(value: str) -> bool:
    return ipaddress.ip_address(value).is_global


def validate_webhook_host(url: str) -> None:
    """Reject literal private IPs and localhost at request time."""
    if private_webhooks_allowed():
        return
    host = (urlsplit(url).hostname or "").lower()
    if host == "localhost" or host.endswith(".localhost"):
        raise ValueError("webhook_url must not point to localhost")
    try:
        if not _is_public_ip(host):
            raise ValueError("webhook_url must not point to a private address")
    except ValueError as exc:
        if "webhook_url" in str(exc):
            raise
        # Not an IP literal: the resolved address is checked before delivery.


async def resolves_to_public_address(url: str) -> bool:
    """Check every resolved address, so a public name cannot alias an internal host."""
    if private_webhooks_allowed():
        return True
    parts = urlsplit(url)
    if not parts.hostname:
        return False
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(parts.hostname, parts.port or 443)
    except OSError:
        return False
    return bool(infos) and all(_is_public_ip(info[4][0].split("%")[0]) for info in infos)
