"""Webhook delivery worker: independent from job retries.

Runs as its own process (`python -m app.webhook_worker`). Claims pending deliveries
with SELECT ... FOR UPDATE SKIP LOCKED (same pattern as the C++ job claim), POSTs a
signed JSON payload, and retries failed deliveries with exponential backoff up to
TASKFORGE_WEBHOOK_MAX_ATTEMPTS times before marking the delivery 'failed' (the job
itself is unaffected either way -- webhook delivery never re-runs a job).
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import random
import time

import httpx
import psycopg
from psycopg.rows import dict_row

from .config import Settings
from .security import WebhookURLError, validate_webhook_url

log = logging.getLogger("taskforge.webhooks")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [webhook-worker] %(message)s")


class WebhookWorker:
    def __init__(self, settings: Settings, secret: str = "", max_attempts: int = 8,
                base_delay: float = 2.0, max_delay: float = 600.0, batch: int = 20, poll_s: float = 1.0,
                claim_lease_secs: float = 30.0):
        self.settings = settings
        self.secret = secret.encode()
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.batch = batch
        self.poll_s = poll_s
        self.claim_lease_secs = claim_lease_secs

    def sign(self, body: bytes) -> str:
        if not self.secret:
            return ""
        return hmac.new(self.secret, body, hashlib.sha256).hexdigest()

    async def deliver_one(self, client: httpx.AsyncClient, conn: psycopg.Connection, row: dict) -> None:
        job = conn.execute("SELECT status, result, last_error, task_type FROM jobs WHERE id = %s",
                           (row["job_id"],)).fetchone()
        body = json.dumps({"event": row["event"], "job_id": str(row["job_id"]), "task_type": job["task_type"],
                           "status": job["status"], "result": job["result"], "error": job["last_error"],
                           "attempt": row["attempts"] + 1}).encode()
        headers = {"Content-Type": "application/json", "X-TaskForge-Event": row["event"]}
        sig = self.sign(body)
        if sig:
            headers["X-TaskForge-Signature"] = "sha256=" + sig

        try:
            # Re-validate at delivery time: DNS can change between submission and now
            # (TOCTOU on the SSRF check), and this also catches URLs from before the
            # allow-list was tightened.
            validate_webhook_url(row["url"], self.settings)
            resp = await client.post(row["url"], content=body, headers=headers, timeout=10.0,
                                     follow_redirects=False)  # a redirect could repoint at an internal host
            ok = 200 <= resp.status_code < 300
            detail = f"HTTP {resp.status_code}"
        except WebhookURLError as e:
            ok, detail = False, f"rejected: {e}"
        except httpx.HTTPError as e:
            ok, detail = False, f"request error: {e}"

        attempts = row["attempts"] + 1
        if ok:
            conn.execute("UPDATE webhook_deliveries SET status='delivered', attempts=%s, last_error=NULL WHERE id=%s",
                        (attempts, row["id"]))
            log.info("delivered %s for job %s (%s)", row["event"], row["job_id"], detail)
        elif attempts >= self.max_attempts:
            conn.execute("UPDATE webhook_deliveries SET status='failed', attempts=%s, last_error=%s WHERE id=%s",
                        (attempts, detail, row["id"]))
            log.warning("giving up on %s for job %s after %d attempts (%s)", row["event"], row["job_id"], attempts, detail)
        else:
            delay = min(self.max_delay, self.base_delay * 2 ** (attempts - 1)) * random.uniform(0.5, 1.0)
            conn.execute(
                "UPDATE webhook_deliveries SET attempts=%s, last_error=%s, "
                "next_attempt_at = now() + make_interval(secs => %s) WHERE id=%s",
                (attempts, detail, delay, row["id"]))
            log.info("retrying %s for job %s in %.1fs (%s)", row["event"], row["job_id"], delay, detail)
        conn.commit()

    def claim(self, conn: psycopg.Connection) -> list[dict]:
        """Atomically claim up to `batch` due deliveries, exactly like JobStore::claim in
        C++: SELECT ... FOR UPDATE SKIP LOCKED, then push next_attempt_at into the future
        as a lease -- in one transaction that commits before any network call is made. If
        this process crashes mid-delivery, the lease expires and another worker (or this
        one, next cycle) picks the row up again instead of it being stuck forever. Two
        workers polling concurrently never claim the same row (proven in
        test_concurrent_workers_never_claim_the_same_delivery).
        """
        with conn.transaction():
            ids = [r["id"] for r in conn.execute(
                "SELECT id FROM webhook_deliveries WHERE status = 'pending' AND next_attempt_at <= now() "
                "ORDER BY next_attempt_at LIMIT %s FOR UPDATE SKIP LOCKED", (self.batch,)).fetchall()]
            if not ids:
                return []
            return conn.execute(
                "UPDATE webhook_deliveries SET next_attempt_at = now() + make_interval(secs => %s) "
                "WHERE id = ANY(%s) RETURNING id, job_id, url, event, attempts",
                (self.claim_lease_secs, ids)).fetchall()

    async def run_once(self, client: httpx.AsyncClient, conn: psycopg.Connection) -> int:
        rows = self.claim(conn)
        for row in rows:
            await self.deliver_one(client, conn, row)
        return len(rows)

    async def run_forever(self, stop: asyncio.Event | None = None) -> None:
        conn = psycopg.connect(self.settings.dsn, row_factory=dict_row, autocommit=False)
        async with httpx.AsyncClient() as client:
            log.info("started (max_attempts=%d base_delay=%.1fs)", self.max_attempts, self.base_delay)
            while stop is None or not stop.is_set():
                try:
                    n = await self.run_once(client, conn)
                except Exception:
                    log.exception("delivery loop error")
                    conn.rollback()
                    n = 0
                await asyncio.sleep(0 if n >= self.batch else self.poll_s)


def main() -> None:
    settings = Settings.from_env()
    worker = WebhookWorker(
        settings,
        secret=os.getenv("TASKFORGE_WEBHOOK_SECRET", ""),
        max_attempts=int(os.getenv("TASKFORGE_WEBHOOK_MAX_ATTEMPTS", "8")),
        base_delay=float(os.getenv("TASKFORGE_WEBHOOK_BASE_DELAY", "2")),
        max_delay=float(os.getenv("TASKFORGE_WEBHOOK_MAX_DELAY", "600")),
    )
    asyncio.run(worker.run_forever())


if __name__ == "__main__":
    main()
