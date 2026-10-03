"""job_store.py (Python) against real PostgreSQL: proves the AI worker speaks the
same claim/complete/retry contract as the C++ scheduler, and end-to-end job runs."""
import json

import pytest

from job_store import JobStore
from main import run_job


def add(conn, task_type="classify_document", payload=None, priority=5, max_retries=3):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO jobs (task_type, payload, priority, max_retries) VALUES (%s, %s, %s, %s) RETURNING id",
            (task_type, json.dumps(payload or {"text": "Invoice total due 500 net 30"}), priority, max_retries))
        jid = cur.fetchone()[0]
    conn.commit()
    return str(jid)


def scalar(conn, sql, params=()):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def test_claim_sets_running_lease_and_history(conn):
    jid = add(conn)
    store = JobStore(conn)
    jobs = store.claim("py-worker-1", 1, 60)
    assert len(jobs) == 1 and jobs[0].id == jid and jobs[0].attempt == 1
    assert scalar(conn, "SELECT status FROM jobs WHERE id=%s", (jid,)) == "running"
    assert scalar(conn, "SELECT worker_id FROM jobs WHERE id=%s", (jid,)) == "py-worker-1"
    assert scalar(conn, "SELECT count(*) FROM job_attempts WHERE job_id=%s", (jid,)) == 1


def test_complete_is_fenced(conn):
    jid = add(conn)
    store = JobStore(conn)
    store.claim("w1", 1, 60)
    assert store.complete(jid, "intruder", 1, {"x": 1}) is False
    assert store.complete(jid, "w1", 1, {"category": "invoice"}) is True
    assert scalar(conn, "SELECT status FROM jobs WHERE id=%s", (jid,)) == "succeeded"
    assert scalar(conn, "SELECT result->>'category' FROM jobs WHERE id=%s", (jid,)) == "invoice"
    assert store.complete(jid, "w1", 1, {}) is False  # already completed


def test_retry_or_dead_matches_cpp_semantics(conn):
    jid = add(conn, max_retries=1)  # 1 retry allowed => dies on the 2nd failed attempt
    store = JobStore(conn)
    store.claim("w1", 1, 60)
    assert store.retry_or_dead(jid, "w1", 1, "temp fail") == "retried"
    assert scalar(conn, "SELECT status FROM jobs WHERE id=%s", (jid,)) == "retrying"
    with conn.cursor() as cur:
        cur.execute("UPDATE jobs SET run_at = now() WHERE id=%s", (jid,))
    conn.commit()
    store.claim("w2", 1, 60)
    assert store.retry_or_dead(jid, "w2", 2, "temp fail again") == "dead"
    assert scalar(conn, "SELECT status FROM jobs WHERE id=%s", (jid,)) == "dead"
    assert scalar(conn, "SELECT reason FROM dead_letters WHERE job_id=%s", (jid,)) == "retries_exhausted"


def test_end_to_end_classification_succeeds(conn, classifier):
    jid = add(conn, payload={"text": "This Agreement is entered into between Party A and Party B governing law herein."})
    store = JobStore(conn)
    job = store.claim("ai-worker-1", 1, 60)[0]
    run_job(store, classifier, job, 'ai-worker-1')
    assert scalar(conn, "SELECT status FROM jobs WHERE id=%s", (jid,)) == "succeeded"
    result = scalar(conn, "SELECT result FROM jobs WHERE id=%s", (jid,))
    assert result["category"] == "contract"
    assert scalar(conn, "SELECT message FROM job_logs WHERE job_id=%s", (jid,)) == "classifying 80 characters of text"


def test_end_to_end_bad_payload_is_permanent_error(conn, classifier):
    jid = add(conn, payload={"text": "   "})  # blank text
    store = JobStore(conn)
    job = store.claim("ai-worker-1", 1, 60)[0]
    run_job(store, classifier, job, 'ai-worker-1')
    assert scalar(conn, "SELECT status FROM jobs WHERE id=%s", (jid,)) == "dead"
    assert scalar(conn, "SELECT reason FROM dead_letters WHERE job_id=%s", (jid,)) == "permanent_error"


def test_end_to_end_rejects_unknown_field(conn, classifier):
    jid = add(conn, payload={"text": "hello", "extra": 1})
    store = JobStore(conn)
    job = store.claim("ai-worker-1", 1, 60)[0]
    run_job(store, classifier, job, 'ai-worker-1')
    assert scalar(conn, "SELECT status FROM jobs WHERE id=%s", (jid,)) == "dead"
    assert "unknown field" in scalar(conn, "SELECT last_error FROM jobs WHERE id=%s", (jid,))


def test_end_to_end_oversized_text_is_permanent_error(conn, classifier):
    jid = add(conn, payload={"text": "x" * 25000})
    store = JobStore(conn)
    job = store.claim("ai-worker-1", 1, 60)[0]
    run_job(store, classifier, job, 'ai-worker-1')
    assert scalar(conn, "SELECT status FROM jobs WHERE id=%s", (jid,)) == "dead"
    assert "exceeds" in scalar(conn, "SELECT last_error FROM jobs WHERE id=%s", (jid,))


def test_wrong_task_type_routed_to_ai_worker_is_dead_lettered(conn, classifier):
    jid = add(conn, task_type="flaky_api", payload={"endpoint": "x"})
    store = JobStore(conn)
    job = store.claim("ai-worker-1", 1, 60)[0]
    run_job(store, classifier, job, 'ai-worker-1')
    assert scalar(conn, "SELECT status FROM jobs WHERE id=%s", (jid,)) == "dead"
    assert scalar(conn, "SELECT reason FROM dead_letters WHERE job_id=%s", (jid,)) == "wrong_worker_pool"


def test_concurrent_python_and_simulated_cpp_claim_never_collide(conn):
    # Two JobStore instances (as if one were the C++ scheduler, one the AI worker)
    # sharing the same queue must never claim the same row.
    import psycopg
    from tests.conftest import DSN
    ids = [add(conn, priority=i % 3) for i in range(20)]
    conn2 = psycopg.connect(DSN, autocommit=False)
    try:
        s1, s2 = JobStore(conn), JobStore(conn2)
        got1 = [j.id for j in s1.claim("w1", 10, 60)]
        got2 = [j.id for j in s2.claim("w2", 10, 60)]
        got1 += [j.id for j in s1.claim("w1", 10, 60)]
        got2 += [j.id for j in s2.claim("w2", 10, 60)]
        assert len(set(got1) & set(got2)) == 0
        assert len(set(got1) | set(got2)) == 20
    finally:
        conn2.close()
