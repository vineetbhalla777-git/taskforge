"""Runtime configuration, read from environment variables (12-factor style)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Settings:
    dsn: str = "host=localhost dbname=taskforge user=taskforge password=taskforge"
    api_keys: tuple[str, ...] = ()
    auth_disabled: bool = False          # explicit opt-out for local dev only
    max_body_bytes: int = 64 * 1024      # request body cap (HTTP layer)
    max_payload_bytes: int = 16 * 1024   # serialized job payload cap
    max_retries_limit: int = 10
    max_delay_days: int = 365
    # Webhook destination policy (SSRF protection)
    webhook_allow_http: bool = False
    webhook_allow_private: bool = False  # allow loopback/private ranges (dev/compose only)
    webhook_allowed_hosts: tuple[str, ...] = ()  # if non-empty, ONLY these hosts are allowed
    db_pool_max: int = 10
    cors_allow_origins: tuple[str, ...] = ()  # e.g. the dashboard's own origin; empty = disabled

    @staticmethod
    def from_env() -> "Settings":
        keys = tuple(k.strip() for k in os.getenv("TASKFORGE_API_KEYS", "").split(",") if k.strip())
        hosts = tuple(h.strip().lower() for h in os.getenv("TASKFORGE_WEBHOOK_ALLOWED_HOSTS", "").split(",") if h.strip())
        origins = tuple(o.strip() for o in os.getenv("TASKFORGE_CORS_ORIGINS", "").split(",") if o.strip())
        d = Settings()
        return Settings(
            dsn=os.getenv("TASKFORGE_DSN", d.dsn),
            api_keys=keys,
            auth_disabled=_bool("TASKFORGE_AUTH_DISABLED"),
            max_body_bytes=int(os.getenv("TASKFORGE_MAX_BODY_BYTES", d.max_body_bytes)),
            max_payload_bytes=int(os.getenv("TASKFORGE_MAX_PAYLOAD_BYTES", d.max_payload_bytes)),
            webhook_allow_http=_bool("TASKFORGE_WEBHOOK_ALLOW_HTTP"),
            webhook_allow_private=_bool("TASKFORGE_WEBHOOK_ALLOW_PRIVATE"),
            webhook_allowed_hosts=hosts,
            db_pool_max=int(os.getenv("TASKFORGE_DB_POOL_MAX", d.db_pool_max)),
            cors_allow_origins=origins,
        )
