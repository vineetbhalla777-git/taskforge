import asyncio
import socket
import uuid

import httpx
import psycopg
import pytest
from psycopg.rows import dict_row

from app.webhook_worker import WebhookWorker
from tests.conftest import DSN, settings as settings_fixture  # noqa: F401


@pytest.fixture(autouse=True)
def fake_dns(monkeypatch):
    table = {"hooks.example.com": "93.184.216.34"}
    real = socket.getaddrinfo

    def fake(host, port, *a, **k):
        if host == "localhost":
            return real(host, port, *a, **k)
        if host not in table:
            raise socket.gaierror("nope")
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (table[host], port))]
    monkeypatch.setattr(socket, "getaddrinfo", fake)


@pytest.fixture()
def dictconn(db):
    # The webhook worker expects dict rows (matches its production connection);
    # `db` (tuple rows) is used for setup/assertions. Same DSN + autocommit-per-
    # statement semantics on `db`, so writes on either connection are immediately
    # visible on the other.
    with psycopg.connect(DSN, row_factory=dict_row, autocommit=True) as c:
        yield c


def make_job(db, webhook_url="https://hooks.example.com/notify"):
    return db.execute(
        "INSERT INTO jobs (task_type, payload, webhook_url) VALUES ('flaky_api', '{}', %s) RETURNING id",
        (webhook_url,)).fetchone()[0]


# ---------------------------------------------------------------- DB trigger
def test_trigger_enqueues_on_succeeded_and_dead_only(db):
    ok_job = make_job(db)
    dead_job = make_job(db)
    cancelled_job = make_job(db)
    no_hook_job = db.execute("INSERT INTO jobs (task_type) VALUES ('flaky_api') RETURNING id").fetchone()[0]

    db.execute("UPDATE jobs SET status='running', worker_id='w', lease_expires_at=now()+interval '1 min' "
              "WHERE id=%s", (ok_job,))                                        # queued->running: no event
    db.execute("UPDATE jobs SET status='succeeded' WHERE id=%s", (ok_job,))     # ->succeeded: event
    db.execute("UPDATE jobs SET status='dead' WHERE id=%s", (dead_job,))        # ->dead: event
    db.execute("UPDATE jobs SET status='cancelled' WHERE id=%s", (cancelled_job,))  # ->cancelled: no event
    db.execute("UPDATE jobs SET status='succeeded' WHERE id=%s", (no_hook_job,))    # no webhook_url: no event

    rows = db.execute("SELECT job_id, event FROM webhook_deliveries ORDER BY event").fetchall()
    assert rows == [(dead_job, "job.dead"), (ok_job, "job.succeeded")]


def test_trigger_does_not_duplicate_on_repeat_or_noop_update(db):
    job = make_job(db)
    db.execute("UPDATE jobs SET status='succeeded' WHERE id=%s", (job,))
    db.execute("UPDATE jobs SET status='succeeded' WHERE id=%s", (job,))  # same status again: no new row (OLD==NEW)
    db.execute("UPDATE jobs SET result='{\"x\":1}' WHERE id=%s", (job,))  # unrelated column change: no new row
    assert db.execute("SELECT count(*) FROM webhook_deliveries WHERE job_id=%s", (job,)).fetchone()[0] == 1


def test_delivery_is_idempotent_per_event_by_unique_constraint(db):
    job = make_job(db)
    db.execute("INSERT INTO webhook_deliveries (job_id, url, event) VALUES (%s, 'https://x/y', 'job.succeeded')", (job,))
    with pytest.raises(Exception):
        db.execute("INSERT INTO webhook_deliveries (job_id, url, event) VALUES (%s, 'https://x/y', 'job.succeeded')", (job,))
    db.rollback()


# ---------------------------------------------------------------- worker delivery
class _Handler:
    """Minimal ASGI app recording every request it receives."""
    def __init__(self, script):
        self.script = list(script)  # list of (status_code, ) to return in order; StopIteration => 200
        self.calls = []

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        code = self.script.pop(0) if self.script else 200
        return httpx.Response(code, json={"ok": code < 300})


@pytest.fixture()
def worker(settings):
    return WebhookWorker(settings, secret="topsecret", max_attempts=3, base_delay=0.01, max_delay=0.05, batch=10)


def enqueue(db, job_id, url="https://hooks.example.com/notify", event="job.succeeded"):
    db.execute("INSERT INTO webhook_deliveries (job_id, url, event) VALUES (%s, %s, %s)", (job_id, url, event))


def test_successful_delivery_marks_delivered_and_signs_payload(db, worker, dictconn):
    job = db.execute("INSERT INTO jobs (task_type, status, result, webhook_url) "
                     "VALUES ('flaky_api', 'succeeded', '{\"n\":1}', 'https://hooks.example.com/notify') "
                     "RETURNING id").fetchone()[0]
    enqueue(db, job)
    handler = _Handler([200])
    transport = httpx.MockTransport(handler)

    async def go():
        async with httpx.AsyncClient(transport=transport) as client:
            n = await worker.run_once(client, dictconn)
            assert n == 1
    asyncio.run(go())

    row = db.execute("SELECT status, attempts FROM webhook_deliveries WHERE job_id=%s", (job,)).fetchone()
    assert row == ("delivered", 1)
    req = handler.calls[0]
    assert req.headers["X-TaskForge-Event"] == "job.succeeded"
    sig = req.headers["X-TaskForge-Signature"]
    import hashlib, hmac as hmac_mod
    expected = "sha256=" + hmac_mod.new(b"topsecret", req.content, hashlib.sha256).hexdigest()
    assert sig == expected
    import json
    assert json.loads(req.content)["status"] == "succeeded"


def test_failed_delivery_retries_then_gives_up(db, worker, dictconn):
    job = db.execute("INSERT INTO jobs (task_type, status, webhook_url) "
                     "VALUES ('flaky_api', 'dead', 'https://hooks.example.com/notify') RETURNING id").fetchone()[0]
    enqueue(db, job, event="job.dead")
    handler = _Handler([503, 503, 503])  # max_attempts=3 => fail, fail, fail => give up
    transport = httpx.MockTransport(handler)

    async def go():
        async with httpx.AsyncClient(transport=transport) as client:
            for _ in range(3):
                db.execute("UPDATE webhook_deliveries SET next_attempt_at = now() WHERE job_id=%s", (job,))
                await worker.run_once(client, dictconn)
    asyncio.run(go())

    row = db.execute("SELECT status, attempts, last_error FROM webhook_deliveries WHERE job_id=%s", (job,)).fetchone()
    assert row[0] == "failed" and row[1] == 3 and "503" in row[2]
    assert len(handler.calls) == 3


def test_eventual_success_after_transient_failures(db, worker, dictconn):
    job = db.execute("INSERT INTO jobs (task_type, status, webhook_url) "
                     "VALUES ('flaky_api', 'succeeded', 'https://hooks.example.com/notify') RETURNING id").fetchone()[0]
    enqueue(db, job)
    handler = _Handler([503, 200])
    transport = httpx.MockTransport(handler)

    async def go():
        async with httpx.AsyncClient(transport=transport) as client:
            await worker.run_once(client, dictconn)
            db.execute("UPDATE webhook_deliveries SET next_attempt_at = now() WHERE job_id=%s", (job,))
            await worker.run_once(client, dictconn)
    asyncio.run(go())

    row = db.execute("SELECT status, attempts FROM webhook_deliveries WHERE job_id=%s", (job,)).fetchone()
    assert row == ("delivered", 2)


def test_worker_rejects_ssrf_destination_at_delivery_time(db, worker, dictconn):
    # A webhook that was valid at submission time but now resolves somewhere forbidden
    # (or was inserted directly, bypassing the API) must still never be called.
    job = db.execute("INSERT INTO jobs (task_type, status, webhook_url) "
                     "VALUES ('flaky_api', 'succeeded', 'http://169.254.169.254/steal') RETURNING id").fetchone()[0]
    enqueue(db, job, url="http://169.254.169.254/steal")
    handler = _Handler([200])
    transport = httpx.MockTransport(handler)

    async def go():
        async with httpx.AsyncClient(transport=transport) as client:
            await worker.run_once(client, dictconn)
    asyncio.run(go())

    assert handler.calls == []  # never actually requested
    row = db.execute("SELECT status, last_error FROM webhook_deliveries WHERE job_id=%s", (job,)).fetchone()
    assert row[0] == "pending" and "rejected" in row[1]


def test_worker_does_not_follow_redirects(db, worker, dictconn):
    job = db.execute("INSERT INTO jobs (task_type, status, webhook_url) "
                     "VALUES ('flaky_api', 'succeeded', 'https://hooks.example.com/notify') RETURNING id").fetchone()[0]
    enqueue(db, job)

    async def handler(request):
        return httpx.Response(302, headers={"Location": "http://169.254.169.254/"})
    transport = httpx.MockTransport(handler)

    async def go():
        async with httpx.AsyncClient(transport=transport) as client:
            await worker.run_once(client, dictconn)
    asyncio.run(go())
    # A 302 is not 2xx, so it's treated as a failure and retried -- never transparently followed.
    row = db.execute("SELECT status FROM webhook_deliveries WHERE job_id=%s", (job,)).fetchone()
    assert row[0] == "pending"


def test_concurrent_workers_never_claim_the_same_delivery(db, settings):
    jobs = [db.execute("INSERT INTO jobs (task_type, status, webhook_url) "
                       "VALUES ('flaky_api', 'succeeded', 'https://hooks.example.com/notify') RETURNING id").fetchone()[0]
            for _ in range(30)]
    for j in jobs:
        enqueue(db, j)
    workers = [WebhookWorker(settings, max_attempts=3, base_delay=0.01, batch=5) for _ in range(4)]
    conns = [psycopg.connect(DSN, row_factory=dict_row, autocommit=True) for _ in range(4)]
    try:
        claimed_ids = []
        for w, c in zip(workers, conns):
            claimed_ids.append([r["id"] for r in w.claim(c)])
        # Keep claiming (simulating 4 pollers racing) until the queue is drained.
        while True:
            more = False
            for i, (w, c) in enumerate(zip(workers, conns)):
                got = [r["id"] for r in w.claim(c)]
                if got:
                    claimed_ids[i].extend(got)
                    more = True
            if not more:
                break
        flat = [x for sub in claimed_ids for x in sub]
        assert len(flat) == 30
        assert len(set(flat)) == 30  # no duplicates across workers
    finally:
        for c in conns:
            c.close()


def test_multiple_pending_deliveries_processed_in_one_batch(db, worker, dictconn):
    jobs = [db.execute("INSERT INTO jobs (task_type, status, webhook_url) "
                       "VALUES ('flaky_api', 'succeeded', 'https://hooks.example.com/notify') RETURNING id").fetchone()[0]
            for _ in range(5)]
    for j in jobs:
        enqueue(db, j)
    handler = _Handler([200] * 5)
    transport = httpx.MockTransport(handler)

    async def go():
        async with httpx.AsyncClient(transport=transport) as client:
            n = await worker.run_once(client, dictconn)
            assert n == 5
    asyncio.run(go())
    assert db.execute("SELECT count(*) FROM webhook_deliveries WHERE status='delivered'").fetchone()[0] == 5
