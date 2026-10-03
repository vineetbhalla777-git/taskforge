"""AI document-classification worker (Phase 8, optional).

Polls for `classify_document` jobs using the same claim/complete/retry contract as
the C++ scheduler (see job_store.py), runs them through a real PyTorch model
(model.py), and reports results back -- indistinguishable, from the job queue's
point of view, from a job the C++ engine ran.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import socket
import time
from pathlib import Path

import psycopg

from job_store import JobStore
from model import Classifier

log = logging.getLogger("taskforge.ai_worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [ai-worker] %(message)s")

MAX_TEXT_CHARS = 20_000
stop_requested = False


def _handle_signal(signum, frame):
    global stop_requested
    stop_requested = True


def run_job(store: JobStore, classifier: Classifier, job, worker_id: str) -> None:
    error, reason, result = None, None, None
    try:
        if job.task_type != "classify_document":
            raise LookupError(f"no handler in this worker for task_type={job.task_type!r}")
        payload = job.payload if isinstance(job.payload, dict) else json.loads(job.payload)
        if set(payload) - {"text"}:
            raise ValueError(f"invalid payload: unknown field(s) {set(payload) - {'text'}}")
        text = payload.get("text")
        if not isinstance(text, str) or not text.strip():
            raise ValueError("invalid payload: 'text' must be a non-empty string")
        if len(text) > MAX_TEXT_CHARS:
            raise ValueError(f"invalid payload: 'text' exceeds {MAX_TEXT_CHARS} characters")
        store.add_log(job.id, job.attempt, "info", f"classifying {len(text)} characters of text")
        result = classifier.classify(text)
    except LookupError as e:
        error, reason = str(e), "wrong_worker_pool"
    except ValueError as e:
        error, reason = str(e), "permanent_error"
    except Exception as e:  # model errors are unexpected but treated as retryable
        error, reason = str(e), "handler_error"

    if error is None:
        if not store.complete(job.id, worker_id, job.attempt, result):
            log.warning("lost ownership of %s before completing it", job.id)
    else:
        store.add_log(job.id, job.attempt, "error", error)
        if reason in ("permanent_error", "wrong_worker_pool"):
            store.fail_permanent(job.id, worker_id, job.attempt, error, reason)
        else:
            store.retry_or_dead(job.id, worker_id, job.attempt, error)


WORKER_ID = os.getenv("TASKFORGE_WORKER_ID", f"{socket.gethostname()}-ai-{os.getpid()}")


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    dsn = os.environ["TASKFORGE_DSN"]
    concurrency = int(os.getenv("TASKFORGE_AI_CONCURRENCY", "1"))
    poll_s = float(os.getenv("TASKFORGE_AI_POLL_S", "0.5"))
    lease_s = int(os.getenv("TASKFORGE_AI_LEASE_SECONDS", "60"))
    model_cache = Path(os.getenv("TASKFORGE_AI_MODEL_CACHE", "/data/model/classifier.pt"))

    log.info("loading model (worker_id=%s)...", WORKER_ID)
    classifier = Classifier(model_cache)
    log.info("model ready, vocab size=%d", len(classifier.vocab))

    conn = psycopg.connect(dsn, autocommit=False)
    store = JobStore(conn)
    last_heartbeat = 0.0

    while not stop_requested:
        now = time.monotonic()
        if now - last_heartbeat > 5:
            store.heartbeat(WORKER_ID, socket.gethostname(), concurrency)
            last_heartbeat = now
        jobs = store.claim(WORKER_ID, concurrency, lease_s)
        if not jobs:
            time.sleep(poll_s)
            continue
        for job in jobs:
            run_job(store, classifier, job, WORKER_ID)  # validates task_type internally; see LookupError above

    log.info("shutting down")


if __name__ == "__main__":
    main()
