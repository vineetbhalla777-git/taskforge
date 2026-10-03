"""Python port of cpp/src/job_store.cpp's claim/complete/fail_permanent/retry_or_dead.

Deliberately mirrors the C++ SQL statement-for-statement (same CTE structure, same
fencing by worker_id + attempt) rather than reusing a shared library, to demonstrate
that any language can be a TaskForge worker: the contract lives entirely in the SQL
against the `jobs` table, not in a client library. If you change the retry/backoff
policy, change it in both places (or better: extract it into a stored procedure).
"""
from __future__ import annotations

import random
from dataclasses import dataclass

import psycopg


@dataclass
class ClaimedJob:
    id: str
    task_type: str
    payload: dict
    priority: int
    attempt: int
    max_retries: int


class JobStore:
    def __init__(self, conn: psycopg.Connection):
        self.conn = conn

    def claim(self, worker_id: str, limit: int, lease_seconds: int) -> list[ClaimedJob]:
        if limit <= 0:
            return []
        sql = """
        WITH picked AS (
          SELECT id FROM jobs WHERE status IN ('queued','retrying') AND run_at <= now()
           ORDER BY priority DESC, run_at ASC, created_at ASC LIMIT %s FOR UPDATE SKIP LOCKED
        ), upd AS (
          UPDATE jobs j SET status = 'running', worker_id = %s, attempts = j.attempts + 1,
                            lease_expires_at = now() + make_interval(secs => %s), started_at = now(), updated_at = now()
            FROM picked WHERE j.id = picked.id
          RETURNING j.id, j.task_type, j.payload, j.priority, j.attempts, j.max_retries
        ), hist AS (INSERT INTO job_attempts (job_id, attempt, worker_id) SELECT id, attempts, %s FROM upd)
        SELECT id, task_type, payload, priority, attempts, max_retries FROM upd
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (limit, worker_id, lease_seconds, worker_id))
            rows = cur.fetchall()
        self.conn.commit()
        jobs = [ClaimedJob(str(r[0]), r[1], r[2], r[3], r[4], r[5]) for r in rows]
        jobs.sort(key=lambda j: -j.priority)  # RETURNING doesn't preserve the CTE's ORDER BY
        return jobs

    def complete(self, job_id: str, worker_id: str, attempt: int, result: dict) -> bool:
        sql = """
        WITH upd AS (
          UPDATE jobs SET status = 'succeeded', result = %s::jsonb, finished_at = now(),
                          lease_expires_at = NULL, last_error = NULL, updated_at = now()
           WHERE id = %s::uuid AND worker_id = %s AND attempts = %s AND status = 'running' RETURNING id
        )
        UPDATE job_attempts a SET finished_at = now(), outcome = 'succeeded',
               duration_ms = (EXTRACT(EPOCH FROM (now() - a.started_at)) * 1000)::int
          FROM upd WHERE a.job_id = upd.id AND a.attempt = %s
        """
        import json
        with self.conn.cursor() as cur:
            cur.execute(sql, (json.dumps(result), job_id, worker_id, attempt, attempt))
            n = cur.rowcount
        self.conn.commit()
        return n > 0

    def fail_permanent(self, job_id: str, worker_id: str, attempt: int, error: str, reason: str) -> bool:
        sql = """
        WITH upd AS (
          UPDATE jobs SET status = 'dead', last_error = %s, finished_at = now(),
                          lease_expires_at = NULL, updated_at = now()
           WHERE id = %s::uuid AND worker_id = %s AND attempts = %s AND status = 'running'
          RETURNING id, attempts, payload
        ), dl AS (
          INSERT INTO dead_letters (job_id, reason, attempts, last_error, payload)
          SELECT id, %s, attempts, %s, payload FROM upd
          ON CONFLICT (job_id) DO UPDATE SET moved_at = now(), reason = EXCLUDED.reason,
                                             attempts = EXCLUDED.attempts, last_error = EXCLUDED.last_error
        )
        UPDATE job_attempts a SET finished_at = now(), outcome = 'failed', error = %s,
               duration_ms = (EXTRACT(EPOCH FROM (now() - a.started_at)) * 1000)::int
          FROM upd WHERE a.job_id = upd.id AND a.attempt = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (error[:4000], job_id, worker_id, attempt, reason, error[:4000], error[:4000], attempt))
            n = cur.rowcount
        self.conn.commit()
        return n > 0

    def retry_or_dead(self, job_id: str, worker_id: str, attempt: int, error: str,
                      base_delay_secs: float = 2.0, max_delay_secs: float = 300.0) -> str:
        """Returns 'retried', 'dead', or 'not_owned' (matches JobStore::RetryOutcome)."""
        sql = """
        WITH target AS (
          SELECT id, max_retries, attempts,
                 least(%s, %s * power(2, greatest(attempts - 1, 0))) * (0.5 + random() * 0.5) AS delay_secs
            FROM jobs WHERE id = %s::uuid AND worker_id = %s AND attempts = %s AND status = 'running' FOR UPDATE
        ), retried AS (
          UPDATE jobs j SET status = 'retrying', last_error = %s, worker_id = NULL, lease_expires_at = NULL,
                            run_at = now() + make_interval(secs => t.delay_secs), updated_at = now()
            FROM target t WHERE j.id = t.id AND t.attempts <= t.max_retries RETURNING j.id
        ), dead AS (
          UPDATE jobs j SET status = 'dead', last_error = %s, finished_at = now(), lease_expires_at = NULL, updated_at = now()
            FROM target t WHERE j.id = t.id AND t.attempts > t.max_retries RETURNING j.id, j.attempts, j.payload
        ), dl AS (
          INSERT INTO dead_letters (job_id, reason, attempts, last_error, payload)
          SELECT id, 'retries_exhausted', attempts, %s, payload FROM dead
          ON CONFLICT (job_id) DO UPDATE SET moved_at = now(), reason = EXCLUDED.reason,
                                             attempts = EXCLUDED.attempts, last_error = EXCLUDED.last_error
        ), hist AS (
          UPDATE job_attempts a SET finished_at = now(), error = %s, outcome = 'failed',
                 duration_ms = (EXTRACT(EPOCH FROM (now() - a.started_at)) * 1000)::int
            FROM target t WHERE a.job_id = t.id AND a.attempt = %s
        )
        SELECT (SELECT count(*) FROM retried) AS n_retried, (SELECT count(*) FROM dead) AS n_dead
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (max_delay_secs, base_delay_secs, job_id, worker_id, attempt,
                             error[:4000], error[:4000], error[:4000], error[:4000], attempt))
            n_retried, n_dead = cur.fetchone()
        self.conn.commit()
        if n_retried > 0:
            return "retried"
        if n_dead > 0:
            return "dead"
        return "not_owned"

    def add_log(self, job_id: str, attempt: int, level: str, message: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO job_logs (job_id, attempt, level, message) VALUES (%s::uuid, %s, %s, %s)",
                       (job_id, attempt, level, message[:4000]))
        self.conn.commit()

    def heartbeat(self, worker_id: str, hostname: str, concurrency: int) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO workers (worker_id, hostname, concurrency, last_heartbeat) VALUES (%s, %s, %s, now()) "
                "ON CONFLICT (worker_id) DO UPDATE SET hostname = EXCLUDED.hostname, concurrency = EXCLUDED.concurrency, "
                "last_heartbeat = now()", (worker_id, hostname, concurrency))
        self.conn.commit()
