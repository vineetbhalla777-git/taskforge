-- TaskForge schema. Idempotent: safe to run on every start.
-- Requires PostgreSQL 13+ (gen_random_uuid() is built in).

CREATE TABLE IF NOT EXISTS jobs (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    task_type         text        NOT NULL,
    payload           jsonb       NOT NULL DEFAULT '{}'::jsonb,
    priority          integer     NOT NULL DEFAULT 5 CHECK (priority BETWEEN 0 AND 9),  -- 9 = most urgent
    status            text        NOT NULL DEFAULT 'queued'
                      CHECK (status IN ('queued','running','retrying','succeeded','dead','cancelled')),
    run_at            timestamptz NOT NULL DEFAULT now(),   -- earliest execution time (delayed jobs, backoff)
    attempts          integer     NOT NULL DEFAULT 0,       -- number of times claimed
    max_retries       integer     NOT NULL DEFAULT 3 CHECK (max_retries >= 0),
    worker_id         text,                                 -- current lease holder
    lease_expires_at  timestamptz,                          -- crash-recovery deadline while RUNNING
    idempotency_key   text UNIQUE,                          -- client-supplied dedup key
    result            jsonb,
    last_error        text,
    webhook_url       text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    started_at        timestamptz,
    finished_at       timestamptz,
    CONSTRAINT running_has_lease CHECK (status <> 'running' OR (worker_id IS NOT NULL AND lease_expires_at IS NOT NULL))
);

-- The claim query scans only jobs that can run; a partial index keeps it tiny
-- even when millions of finished jobs accumulate.
CREATE INDEX IF NOT EXISTS jobs_claim_idx
    ON jobs (priority DESC, run_at ASC, created_at ASC)
    WHERE status IN ('queued','retrying');

-- Lease reaper looks only at running jobs.
CREATE INDEX IF NOT EXISTS jobs_lease_idx
    ON jobs (lease_expires_at)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS jobs_status_created_idx ON jobs (status, created_at DESC);
CREATE INDEX IF NOT EXISTS jobs_task_type_idx      ON jobs (task_type);

-- One row per execution attempt: the job's history.
CREATE TABLE IF NOT EXISTS job_attempts (
    id           bigserial PRIMARY KEY,
    job_id       uuid        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt      integer     NOT NULL,
    worker_id    text        NOT NULL,
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    outcome      text        NOT NULL DEFAULT 'running'
                 CHECK (outcome IN ('running','succeeded','failed','lease_expired','cancelled')),
    error        text,
    duration_ms  integer,
    UNIQUE (job_id, attempt)
);

-- Execution log lines emitted by handlers.
CREATE TABLE IF NOT EXISTS job_logs (
    id       bigserial PRIMARY KEY,
    job_id   uuid        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt  integer,
    ts       timestamptz NOT NULL DEFAULT now(),
    level    text        NOT NULL DEFAULT 'info',
    message  text        NOT NULL
);
CREATE INDEX IF NOT EXISTS job_logs_job_idx ON job_logs (job_id, id);

-- Jobs that exhausted their retries. Kept for inspection and manual requeue.
CREATE TABLE IF NOT EXISTS dead_letters (
    job_id      uuid PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    moved_at    timestamptz NOT NULL DEFAULT now(),
    reason      text        NOT NULL,
    attempts    integer     NOT NULL,
    last_error  text,
    payload     jsonb       NOT NULL
);

-- Webhook notifications are retried independently of the job itself.
CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id             bigserial PRIMARY KEY,
    job_id         uuid        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    url            text        NOT NULL,
    event          text        NOT NULL,             -- job.succeeded | job.dead
    status         text        NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending','delivered','failed')),
    attempts       integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error     text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (job_id, event)
);
CREATE INDEX IF NOT EXISTS webhook_pending_idx ON webhook_deliveries (next_attempt_at) WHERE status = 'pending';

-- Whenever a job with a webhook_url reaches a terminal state, enqueue exactly one
-- delivery per event automatically -- regardless of which language changed the row
-- (the C++ scheduler for job.succeeded/job.dead, the API for job.cancelled). This
-- keeps "notify on completion" a single, always-applied rule instead of logic that
-- has to be duplicated (and kept in sync) at every call site that changes status.
CREATE OR REPLACE FUNCTION taskforge_enqueue_webhook() RETURNS trigger AS $$
BEGIN
    IF NEW.webhook_url IS NOT NULL AND NEW.status IN ('succeeded', 'dead')
       AND (OLD.status IS DISTINCT FROM NEW.status) THEN
        INSERT INTO webhook_deliveries (job_id, url, event)
        VALUES (NEW.id, NEW.webhook_url, 'job.' || NEW.status)
        ON CONFLICT (job_id, event) DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS jobs_webhook_trigger ON jobs;
CREATE TRIGGER jobs_webhook_trigger
    AFTER UPDATE ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION taskforge_enqueue_webhook();

-- Worker registry, refreshed by heartbeats.
CREATE TABLE IF NOT EXISTS workers (
    worker_id       text PRIMARY KEY,
    hostname        text        NOT NULL,
    concurrency     integer     NOT NULL,
    started_at      timestamptz NOT NULL DEFAULT now(),
    last_heartbeat  timestamptz NOT NULL DEFAULT now()
);
