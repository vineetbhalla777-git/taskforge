"""TaskForge REST API."""
from __future__ import annotations

import json
import os
import uuid
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Gauge, generate_latest
from fastapi.responses import JSONResponse
from psycopg.types.json import Jsonb
from pydantic import ValidationError

from .config import Settings
from .db import apply_schema, make_pool
from .schemas import DeadLetter, Job, JobCreate, JobDetail, JobList, LogLine, Status
from .security import WebhookURLError, make_auth_dependency, validate_webhook_url
from .tasks import TASK_TYPES

JOB_COLS = ("id, task_type, payload, priority, status, run_at, attempts, max_retries, worker_id, "
            "lease_expires_at, idempotency_key, result, last_error, webhook_url, created_at, "
            "updated_at, started_at, finished_at")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    auth = make_auth_dependency(settings)  # raises if misconfigured: fail closed

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool = make_pool(settings.dsn, settings.db_pool_max)
        pool.open(wait=True, timeout=30)
        if os.getenv("TASKFORGE_APPLY_SCHEMA", "0") == "1":
            apply_schema(pool)
        app.state.pool = pool
        yield
        pool.close()

    app = FastAPI(title="TaskForge", version="0.3.0", lifespan=lifespan)
    app.state.settings = settings

    if settings.cors_allow_origins:
        # Origins only, no credentials: the dashboard authenticates with an API key
        # header it attaches itself, not cookies, so wildcard-with-credentials risk
        # doesn't apply -- but we still only allow explicitly configured origins.
        app.add_middleware(CORSMiddleware, allow_origins=list(settings.cors_allow_origins),
                           allow_methods=["*"], allow_headers=["*"], allow_credentials=False)

    # ---- Prometheus metrics ---------------------------------------------------------
    # A dedicated registry (not the global default) so creating a second app instance in
    # the same process -- every pytest test does this -- doesn't collide on metric names.
    registry = CollectorRegistry()
    http_requests = Counter("taskforge_api_requests_total", "API requests", ["method", "path", "status"],
                            registry=registry)
    jobs_submitted = Counter("taskforge_api_jobs_submitted_total", "Jobs accepted via POST /v1/jobs", ["task_type"],
                             registry=registry)
    jobs_by_status = Gauge("taskforge_jobs_by_status", "Current job count by status", ["status"], registry=registry)

    @app.middleware("http")
    async def count_requests(request: Request, call_next):
        response = await call_next(request)
        route = request.scope.get("route")
        path = route.path if route else request.url.path
        http_requests.labels(request.method, path, str(response.status_code)).inc()
        return response

    @app.get("/metrics", include_in_schema=False)
    def metrics(request: Request):
        try:
            with pool_of(request).connection() as conn:
                counts = {r["status"]: r["n"] for r in conn.execute(
                    "SELECT status, count(*)::int AS n FROM jobs GROUP BY status").fetchall()}
            for s in ("queued", "running", "retrying", "succeeded", "dead", "cancelled"):
                jobs_by_status.labels(s).set(counts.get(s, 0))
        except Exception:
            pass  # metrics must never 500 the scrape; Prometheus will just see stale gauges
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    # ---- request size limit (HTTP layer) ------------------------------------------
    def pool_of(request: Request):
        return request.app.state.pool

    Protected = [Depends(auth)]

    @app.middleware("http")
    async def limit_body(request: Request, call_next):
        if request.method in ("POST", "PUT", "PATCH"):
            cl = request.headers.get("content-length")
            if cl is None:
                if request.headers.get("transfer-encoding"):
                    return JSONResponse({"detail": "Content-Length required (chunked bodies not accepted)"}, 411)
            elif not cl.isdigit() or int(cl) > settings.max_body_bytes:
                return JSONResponse({"detail": f"request body exceeds {settings.max_body_bytes} bytes"}, 413)
        return await call_next(request)


    @app.get("/healthz")
    def healthz(request: Request):
        try:
            with pool_of(request).connection() as c:
                c.execute("SELECT 1")
        except Exception:
            raise HTTPException(503, "database unavailable")
        return {"status": "ok"}

    @app.get("/v1/task-types", dependencies=Protected)
    def task_types():
        return {name: model.model_json_schema() for name, model in TASK_TYPES.items()}

    # ---- submit --------------------------------------------------------------------
    @app.post("/v1/jobs", response_model=Job, status_code=201, dependencies=Protected)
    def submit_job(body: JobCreate, request: Request, response: Response,
                   idempotency_key_header: Annotated[str | None, Header(alias="Idempotency-Key")] = None):
        model = TASK_TYPES.get(body.task_type)
        if model is None:
            raise HTTPException(422, f"unknown task_type {body.task_type!r}; registered: {sorted(TASK_TYPES)}")
        try:
            payload = model.model_validate(body.payload).model_dump(mode="json")
        except ValidationError as e:
            raise HTTPException(422, detail=e.errors(include_url=False, include_context=False, include_input=False))
        if len(json.dumps(payload)) > settings.max_payload_bytes:
            raise HTTPException(422, f"payload exceeds {settings.max_payload_bytes} bytes")
        if body.max_retries > settings.max_retries_limit:
            raise HTTPException(422, f"max_retries must be <= {settings.max_retries_limit}")
        if body.run_at is not None and body.delay_seconds is not None:
            raise HTTPException(422, "give run_at or delay_seconds, not both")
        if body.delay_seconds is not None and body.delay_seconds > settings.max_delay_days * 86400:
            raise HTTPException(422, f"delay_seconds must be <= {settings.max_delay_days} days")
        if body.webhook_url is not None:
            try:
                validate_webhook_url(body.webhook_url, settings)
            except WebhookURLError as e:
                raise HTTPException(422, f"webhook_url rejected: {e}")
        key = idempotency_key_header or body.idempotency_key
        if idempotency_key_header and body.idempotency_key and idempotency_key_header != body.idempotency_key:
            raise HTTPException(422, "Idempotency-Key header and body idempotency_key differ")

        with pool_of(request).connection() as conn:
            row = conn.execute(
                f"""INSERT INTO jobs (task_type, payload, priority, run_at, max_retries, webhook_url, idempotency_key)
                    VALUES (%s, %s, %s, COALESCE(%s::timestamptz, now() + make_interval(secs => COALESCE(%s::int, 0))),
                            %s, %s, %s)
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING {JOB_COLS}""",
                (body.task_type, Jsonb(payload), body.priority, body.run_at, body.delay_seconds,
                 body.max_retries, body.webhook_url, key)).fetchone()
            if row is None:  # idempotency-key collision: return the original job if the request matches
                existing = conn.execute(f"SELECT {JOB_COLS} FROM jobs WHERE idempotency_key = %s", (key,)).fetchone()
                if existing["task_type"] != body.task_type or existing["payload"] != payload:
                    raise HTTPException(409, "idempotency key already used with a different request")
                response.status_code = 200
                return existing
            jobs_submitted.labels(body.task_type).inc()
            return row

    # ---- read ----------------------------------------------------------------------
    @app.get("/v1/jobs/{job_id}", response_model=JobDetail, dependencies=Protected)
    def get_job(job_id: uuid.UUID, request: Request):
        with pool_of(request).connection() as conn:
            row = conn.execute(f"SELECT {JOB_COLS} FROM jobs WHERE id = %s", (job_id,)).fetchone()
            if row is None:
                raise HTTPException(404, "job not found")
            row["history"] = conn.execute(
                "SELECT attempt, worker_id, started_at, finished_at, outcome, error, duration_ms "
                "FROM job_attempts WHERE job_id = %s ORDER BY attempt", (job_id,)).fetchall()
            return row

    @app.get("/v1/jobs", response_model=JobList, dependencies=Protected)
    def list_jobs(request: Request, status: Status | None = None, task_type: str | None = None,
                  priority: Annotated[int | None, Query(ge=0, le=9)] = None,
                  limit: Annotated[int, Query(ge=1, le=200)] = 50,
                  offset: Annotated[int, Query(ge=0)] = 0):
        where, params = [], []
        for col, val in (("status", status), ("task_type", task_type), ("priority", priority)):
            if val is not None:
                where.append(f"{col} = %s")
                params.append(val)
        sql = f"SELECT {JOB_COLS} FROM jobs"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at DESC, id LIMIT %s OFFSET %s"
        with pool_of(request).connection() as conn:
            rows = conn.execute(sql, (*params, limit + 1, offset)).fetchall()  # +1 => know if there is a next page
        return {"items": rows[:limit], "limit": limit, "offset": offset,
                "next_offset": offset + limit if len(rows) > limit else None}

    @app.get("/v1/jobs/{job_id}/logs", response_model=list[LogLine], dependencies=Protected)
    def job_logs(job_id: uuid.UUID, request: Request, after_id: int = 0,
                 limit: Annotated[int, Query(ge=1, le=1000)] = 200):
        with pool_of(request).connection() as conn:
            if conn.execute("SELECT 1 FROM jobs WHERE id = %s", (job_id,)).fetchone() is None:
                raise HTTPException(404, "job not found")
            return conn.execute("SELECT id, ts, attempt, level, message FROM job_logs "
                                "WHERE job_id = %s AND id > %s ORDER BY id LIMIT %s",
                                (job_id, after_id, limit)).fetchall()

    # ---- cancel --------------------------------------------------------------------
    @app.post("/v1/jobs/{job_id}/cancel", response_model=Job, dependencies=Protected)
    def cancel_job(job_id: uuid.UUID, request: Request):
        with pool_of(request).connection() as conn:
            # Competes for the row lock with the scheduler's claim: exactly one of them wins.
            row = conn.execute(
                f"""UPDATE jobs SET status = 'cancelled', finished_at = now(), updated_at = now()
                     WHERE id = %s AND status IN ('queued', 'retrying') RETURNING {JOB_COLS}""",
                (job_id,)).fetchone()
            if row:
                return row
            cur = conn.execute("SELECT status FROM jobs WHERE id = %s", (job_id,)).fetchone()
            if cur is None:
                raise HTTPException(404, "job not found")
            raise HTTPException(409, f"job is {cur['status']}; only queued or retrying jobs can be cancelled")

    # ---- dead letters --------------------------------------------------------------
    @app.get("/v1/dead-letters", response_model=list[DeadLetter], dependencies=Protected)
    def list_dead(request: Request, limit: Annotated[int, Query(ge=1, le=200)] = 50,
                  offset: Annotated[int, Query(ge=0)] = 0):
        with pool_of(request).connection() as conn:
            return conn.execute(
                "SELECT d.job_id, j.task_type, d.moved_at, d.reason, d.attempts, d.last_error, d.payload "
                "FROM dead_letters d JOIN jobs j ON j.id = d.job_id ORDER BY d.moved_at DESC LIMIT %s OFFSET %s",
                (limit, offset)).fetchall()

    @app.post("/v1/dead-letters/{job_id}/requeue", response_model=Job, dependencies=Protected)
    def requeue_dead(job_id: uuid.UUID, request: Request):
        with pool_of(request).connection() as conn:
            row = conn.execute(
                f"""WITH d AS (DELETE FROM dead_letters WHERE job_id = %s RETURNING job_id)
                    UPDATE jobs SET status = 'queued', attempts = 0, run_at = now(), last_error = NULL,
                           worker_id = NULL, finished_at = NULL, updated_at = now()
                     WHERE id IN (SELECT job_id FROM d) AND status = 'dead' RETURNING {JOB_COLS}""",
                (job_id,)).fetchone()
            if row is None:
                raise HTTPException(404, "no such dead-letter job")
            return row

    # ---- stats ---------------------------------------------------------------------
    @app.get("/v1/stats", dependencies=Protected)
    def stats(request: Request):
        with pool_of(request).connection() as conn:
            counts = {r["status"]: r["n"] for r in conn.execute(
                "SELECT status, count(*)::int AS n FROM jobs GROUP BY status").fetchall()}
            oldest = conn.execute(
                "SELECT EXTRACT(EPOCH FROM now() - min(run_at))::float AS s FROM jobs "
                "WHERE status IN ('queued','retrying') AND run_at <= now()").fetchone()["s"]
        by_status = {s: counts.get(s, 0) for s in ("queued", "running", "retrying", "succeeded", "dead", "cancelled")}
        return {"by_status": by_status, "oldest_waiting_seconds": oldest}

    return app
