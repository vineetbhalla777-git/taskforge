"""API-key authentication and webhook-destination validation."""
from __future__ import annotations

import hmac
import ipaddress
import socket
from urllib.parse import urlsplit

from fastapi import Header, HTTPException, Request

from .config import Settings


def make_auth_dependency(settings: Settings):
    """Fail closed: the app refuses to start with no keys unless auth is explicitly disabled."""
    if not settings.api_keys and not settings.auth_disabled:
        raise RuntimeError("TASKFORGE_API_KEYS is empty; set keys or TASKFORGE_AUTH_DISABLED=1 (dev only)")
    keys = [k.encode() for k in settings.api_keys]

    def require_api_key(request: Request, x_api_key: str | None = Header(default=None)) -> None:
        if settings.auth_disabled:
            return
        supplied = (x_api_key or "").encode()
        # Compare against every key without short-circuiting (constant-time per comparison).
        ok = False
        for k in keys:
            ok |= hmac.compare_digest(supplied, k)
        if not ok:
            raise HTTPException(status_code=401, detail="invalid or missing API key",
                                headers={"WWW-Authenticate": "ApiKey"})

    return require_api_key


class WebhookURLError(ValueError):
    pass


def _is_forbidden_ip(ip: ipaddress._BaseAddress) -> bool:
    return (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast
            or ip.is_reserved or ip.is_unspecified)


def validate_webhook_url(url: str, settings: Settings) -> str:
    """Reject URLs that could be used for SSRF. Returns the normalised URL.

    Checks scheme, embedded credentials, host allow-list, and that EVERY address the
    host resolves to is public. The delivery worker must repeat the resolution check
    at connect time (DNS can change between submission and delivery).
    """
    if len(url) > 2048:
        raise WebhookURLError("webhook_url too long")
    parts = urlsplit(url)
    allowed = {"https"} | ({"http"} if settings.webhook_allow_http else set())
    if parts.scheme not in allowed:
        raise WebhookURLError(f"webhook scheme must be one of {sorted(allowed)}")
    if parts.username or parts.password:
        raise WebhookURLError("credentials in webhook_url are not allowed")
    host = (parts.hostname or "").lower()
    if not host:
        raise WebhookURLError("webhook_url has no host")
    if settings.webhook_allowed_hosts and host not in settings.webhook_allowed_hosts:
        raise WebhookURLError("webhook host is not in the allow-list")
    if settings.webhook_allow_private:
        return url
    try:
        addrs = [ipaddress.ip_address(host)]
    except ValueError:
        if host == "localhost" or host.endswith(".localhost") or host.endswith(".internal"):
            raise WebhookURLError("webhook host resolves to a forbidden address")
        try:
            infos = socket.getaddrinfo(host, parts.port or (443 if parts.scheme == "https" else 80),
                                       proto=socket.IPPROTO_TCP)
        except socket.gaierror:
            raise WebhookURLError("webhook host does not resolve")
        addrs = [ipaddress.ip_address(i[4][0].split("%")[0]) for i in infos]
    for a in addrs:
        if _is_forbidden_ip(a):
            raise WebhookURLError("webhook host resolves to a forbidden address")
    return url
