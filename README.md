# TaskForge

A distributed background job queue and scheduler (a small Celery/Sidekiq): REST API in,
PostgreSQL as the durable queue, a C++17 engine that claims and executes jobs concurrently.

## Roadmap / status

| # | Phase | Status |
|---|-------|--------|
| 1 | C++ priority thread pool | done, 13 GoogleTest cases, TSan + ASan clean |
| 2 | PostgreSQL persistence + atomic claiming (`SKIP LOCKED`) | done, 8 integration tests, mutation-checked |
| 3 | FastAPI submission/status API | done, 65 pytest cases |
| 4 | Scheduler binary: claim -> dispatch -> execute -> record; 3 real handlers | done, 18 GoogleTest cases + live demo |
| 5 | Retries, exponential backoff, dead letters | done, 3 new C++ tests |
| 6 | Worker heartbeats, leases, crash recovery | done, verified with a real kill -9 mid-job |
| 7 | Dashboard, Prometheus/Grafana, Docker Compose, integration + load tests | done (Compose written but not executed -- no Docker in this sandbox) |
| 8 | Optional PyTorch document-classification worker | done, 19 tests, real trained model |

**All 8 phases are implemented and tested.** See "Known limitations" at the end of this
file for the one thing I could not verify directly (running Docker Compose itself).

## Layout

```
db/schema.sql                 full schema (jobs, attempts, logs, dead_letters, webhooks, workers)
cpp/include/taskforge/        thread_pool.hpp, db.hpp, job_store.hpp
cpp/src/                      thread_pool.cpp, db.cpp, job_store.cpp
cpp/include/taskforge/        handler.hpp, handlers.hpp, scheduler.hpp
cpp/src/                      handlers.cpp (image_resize, generate_report, flaky_api), scheduler.cpp,
                              scheduler_main.cpp (taskforge_scheduler binary), stb_impl.cpp
cpp/tests/                    thread_pool_test.cpp, job_store_test.cpp, handlers_test.cpp, scheduler_test.cpp
cpp/examples/                 pool_bench.cpp, claim_bench.cpp
cpp/third_party/              stb_image*.h (public domain, vendored)
cpp/include/taskforge/        metrics_server.hpp (embedded Prometheus exporter)
api/app/                      config.py, security.py, tasks.py, schemas.py, db.py, main.py, asgi.py,
                              webhook_worker.py
api/tests/                    test_api.py, test_security.py, test_webhooks.py, test_metrics.py (pytest)
ai_worker/                    model.py (PyTorch classifier), job_store.py (Python port of the SQL
                              claim contract), main.py, tests/ (pytest)
dashboard/                    index.html (monitoring UI), Dockerfile, nginx.conf
monitoring/                   prometheus.yml, grafana/ (provisioned datasource + dashboard JSON)
loadtest/                     locustfile.py
docker-compose.yml            full stack: postgres, api, scheduler, webhook-worker, dashboard,
                              prometheus, grafana, plus optional `seed` and `ai` profiles
```

## Build and test (Ubuntu/Debian)

```bash
sudo apt-get install -y cmake g++ libgtest-dev libpq-dev postgresql nlohmann-json3-dev python3-pip
cd cpp && cmake -S . -B build && cmake --build build -j
./build/thread_pool_test                       # Phase 1, no database needed

# Phases 2 and 4 need PostgreSQL 13+ with the schema applied:
psql -c "CREATE USER taskforge PASSWORD 'taskforge' SUPERUSER" -c "CREATE DATABASE taskforge_test OWNER taskforge"
PGPASSWORD=taskforge psql -h localhost -U taskforge -d taskforge_test -f ../db/schema.sql
export TASKFORGE_TEST_DSN="host=localhost dbname=taskforge_test user=taskforge password=taskforge"
./build/job_store_test ./build/handlers_test ./build/scheduler_test   # skipped, not failed, if unset

# Sanitizers
cmake -S . -B build-tsan -DTASKFORGE_SANITIZE=thread -DCMAKE_BUILD_TYPE=Debug && cmake --build build-tsan --target thread_pool_test && ./build-tsan/thread_pool_test

# Phase 3: the API (needs TASKFORGE_TEST_DSN as above)
cd ../api && pip install -r requirements.txt
python3 -m pytest -q

# Run the real system end to end
export TASKFORGE_DSN="host=localhost dbname=taskforge user=taskforge password=taskforge"
../cpp/build/taskforge_scheduler &                       # TASKFORGE_CONCURRENCY, _DATA_DIR, etc. (see scheduler_main.cpp)
TASKFORGE_API_KEYS=dev-key python3 -m uvicorn app.asgi:app --port 8000 &
curl -H "X-API-Key: dev-key" -X POST localhost:8000/v1/jobs \
     -d '{"task_type":"flaky_api","priority":9,"payload":{"endpoint":"pay/charge","failure_rate":0}}'
```

## Phase 1: ThreadPool

- Max-heap of `(priority, sequence)`; **higher priority first, FIFO within a priority**.
- `submit()` returns a `std::future` (exceptions travel through it); `try_post()` is fire-and-forget.
- Optional `max_queue` gives back-pressure (`QueueFullError` / `false`).
- `free_slots()` = threads - active - queued. The scheduler claims exactly this many jobs
  from PostgreSQL, so work is never claimed faster than it can run (an unclaimed job stays
  visible to other schedulers, and a claimed-but-waiting job would burn its lease).
- `shutdown(drain=true|false)`, idempotent; destructor drains.

## Phase 2: durable queue and atomic claiming

`JobStore::claim()` is one SQL statement (CTE): pick eligible rows `ORDER BY priority DESC,
run_at, created_at LIMIT n FOR UPDATE SKIP LOCKED` -> set `running`, `worker_id`,
`lease_expires_at`, `attempts+1` -> insert the `job_attempts` row. It auto-commits, so the
claim is durable before any handler runs. `complete()` is *fenced* by worker id and attempt
number, so a worker whose lease expired cannot overwrite a newer attempt.

Delivery is **at-least-once**: if a worker dies after the handler's side effect but before
`complete()`, the job runs again after lease expiry (Phase 6). Handlers should be idempotent.

## Measured results (sandbox: 1 vCPU shared with PostgreSQL; treat as a floor, not a claim)

Thread pool (`pool_bench`): ~3.1M no-op tasks/s at 1-4 threads, 1.2M/s at 8 (contention on
one core). 10 ms sleeping tasks scale near-ideally: 99/s @1 thread, 395/s @4, 1,579/s @16.
A priority-9 task submitted behind a 2,000-task backlog started in <0.1 ms; a priority -1
task waited ~1.05 s.

Claiming (`claim_bench`, 20,000 jobs, batch 50, local PostgreSQL 16):

| claimers | jobs/s | claim p50 | claim p99 |
|---|---|---|---|
| 1 | 20,756 | 2.25 ms | 3.79 ms |
| 2 | 20,797 | 4.42 ms | 8.64 ms |
| 4 | 20,519 | 8.79 ms | 19.89 ms |
| 8 | 17,264 | 19.99 ms | 44.32 ms |

Throughput does not scale with claimers here because there is one core; latency per call grows
in proportion. Re-run on multi-core hardware before drawing conclusions.

Mutation check: deleting `FOR UPDATE SKIP LOCKED` makes `ConcurrentClaimersNeverGetTheSameJob`
fail (7,350 claims for 2,000 jobs), so the test does detect double-claiming.


## Phase 3: FastAPI service

Every write goes through parameterised SQL (no string-built queries). Highlights:

- **Auth**: `X-API-Key`, checked with `hmac.compare_digest` against every configured key
  (no short-circuit timing signal); the app *refuses to start* if no keys are configured and
  auth was not explicitly disabled — fails closed, not open.
- **Task types are a closed set**: `app/tasks.py` is the only place a payload shape is defined;
  submitting an unregistered `task_type` is a 422. Each payload model uses `extra="forbid"` and
  tight field constraints (regex file names with no `..` or `/`, bounded dimensions, bounded
  latency), so arbitrary shell commands or path traversal cannot reach a handler.
- **Webhook SSRF protection** (`app/security.py`): scheme allow-list (https only, unless a dev
  flag opts into http), no embedded credentials, optional host allow-list, and a DNS resolution
  check that rejects loopback/private/link-local/metadata addresses (e.g. `169.254.169.254`).
- **Idempotency keys**: `UNIQUE` column + `ON CONFLICT DO NOTHING`, so 16 concurrent identical
  requests (tested) produce exactly one job; a matching request gets the same job back (200), a
  conflicting one gets 409.
- **Request-size and payload-size limits**, delayed jobs (`run_at` or `delay_seconds`),
  filtering/pagination on `GET /v1/jobs`, execution history and logs, cancellation (races
  correctly against the scheduler's claim -- exactly one of them wins), dead-letter listing
  and manual requeue, and a `/v1/stats` endpoint the dashboard will use in Phase 7.
- **65 pytest cases**, all against a real PostgreSQL instance (no mocks for the database).

## Phase 4: the scheduler binary and real handlers

`Scheduler::loop()` asks the pool how many slots are free, claims exactly that many jobs
(so priority is decided in SQL at the last possible moment, and a claimed job's lease never
ticks away sitting in an in-memory queue), and posts each to the thread pool by priority.
`run_job()` gives the handler a `JobContext` (id, attempt, a logger writing to `job_logs`, the
DSN and data directory) and, on return, calls `JobStore::complete()`; on exception it calls
`JobStore::fail_permanent()` (retries with backoff are Phase 5 -- every failure is currently
terminal). Both are fenced by worker id and attempt number, matching Phase 2.

Three real handlers, each re-validating its payload independently of the API (defence in depth)
and idempotent under at-least-once delivery (temp-file + rename, so a re-run overwrites the
same output rather than duplicating it):

- **`image_resize`**: decodes with stb_image, resizes with stb_image_resize2 (aspect-preserving
  or stretch), encodes PNG or JPEG with stb_image_write. Rejects oversized images, non-file-name
  sources (`../..`, absolute paths, subdirectories) and corrupt files.
- **`generate_report`**: queries `jobs` directly (its own table) and writes a CSV or JSON summary
  grouped by task type and status, with average runtime.
- **`flaky_api`**: simulates a third-party call with configurable latency and failure rate;
  `fail_first_n_attempts` makes retry demos deterministic, `permanent: true` simulates a
  non-retryable client error (HTTP 400) versus the default retryable one (HTTP 503).

**18 GoogleTest cases** (8 handler unit tests, 10 scheduler integration tests), including:
priority ordering and preemption of a waiting backlog, delayed-job timing, concurrency actually
bounded by thread count, unknown task types and handler exceptions both landing in the dead
letter table, graceful shutdown draining the running job but claiming no more, and two
independent scheduler processes sharing one queue with zero duplicate or lost executions.

**Live end-to-end run** (real PostgreSQL, real HTTP, real files, no mocks): submitted low- and
high-priority `flaky_api` jobs, an `image_resize` job against a real PNG, a `generate_report`
job, and a job engineered to fail permanently. Result: the high-priority job completed in 52 ms;
the image was resized 400x300 -> 100x75 and written to disk; the report was generated with real
row counts from the `jobs` table; the failing job was dead-lettered with its HTTP 400 reason
recorded; a delayed job was submitted and then cancelled successfully.

Known gap going into Phase 5: there is no `retrying` state reachable yet -- every failure is
dead-lettered on the first attempt regardless of `max_retries`. That is next.


## Phase 5: retries with exponential backoff

`JobStore::retry_or_dead()` is one SQL statement that reads the job's own `max_retries`
and decides, atomically and fenced by worker id + attempt (same pattern as `complete()`):
retry with `delay = min(max_delay, base * 2^(attempt-1)) * U(0.5, 1.0)` and status
`retrying`, or dead-letter with reason `retries_exhausted` once the budget is used up.
`PermanentError` always skips straight to `fail_permanent()` regardless of retries
remaining -- a bad payload will never succeed no matter how many times it's retried.
Verified live: a job configured to fail its first two attempts recorded two `failed`
attempts in `job_attempts`, sat in `retrying` with backoff, and would have succeeded on
attempt 3 (demonstrated in the end-to-end run below).

## Phase 6: heartbeats and crash recovery

A second thread per scheduler (`maintenance_loop`) runs on its own DB connection so a
slow claim cycle never delays heartbeats or vice versa. Every `heartbeat_ms` it upserts
the `workers` table; every `reap_interval_ms` it scans for `status='running' AND
lease_expires_at < now()` and runs each one through `retry_or_dead()` -- a crashed job
consumes a retry attempt just like a handler exception, so a job that crashes
repeatedly still eventually reaches the dead letter table.

**Verified with an actual `kill -9`, not a simulated failure**: submitted a 4-second job,
confirmed it was `running` under scheduler A, hard-killed scheduler A's process outright
(no graceful shutdown, no chance for it to record anything), waited for the 3-second
lease to expire, started scheduler B. Result:
```
final status: succeeded  attempts: 2  worker: e2e-scheduler-2
history: [(1, 'e2e-scheduler-1', 'lease_expired'), (2, 'e2e-scheduler-2', 'succeeded')]
```
Exactly the at-least-once semantics the design promises: attempt 1 is marked
`lease_expired`, attempt 2 (by a different process) completes the job.

## Webhooks

A Postgres trigger (`taskforge_enqueue_webhook`) inserts into `webhook_deliveries`
whenever a job with a `webhook_url` transitions to `succeeded` or `dead` -- one rule,
applied regardless of which language changed the row (C++ scheduler or the API's
cancel endpoint), instead of duplicated logic at every call site. `app/webhook_worker.py`
delivers independently: atomic claim-with-lease (same `SELECT ... FOR UPDATE SKIP
LOCKED` pattern as the job queue, proven race-free with 4 concurrent claimers in
`test_concurrent_workers_never_claim_the_same_delivery`), HMAC-SHA256 signed payloads,
SSRF re-validation at delivery time (DNS can change between submission and delivery),
`follow_redirects=False` (a redirect could repoint at an internal address), and its own
exponential backoff independent of the job's retry policy. Verified live: a job with a
webhook pointed at a local HTTP receiver produced exactly the JSON payload expected at
that receiver, no mocks involved.

## Metrics and dashboard

**C++ scheduler**: `MetricsServer` is a ~150-line embedded HTTP server over raw POSIX
sockets (no dependency) serving `/metrics` in Prometheus text format on port 9464 --
`taskforge_jobs_{claimed,succeeded,failed,retried,dead}_total`,
`taskforge_leases_reaped_total`, `taskforge_pool_{threads,active,queued}`. 5 GoogleTest
cases including a real HTTP round-trip over a real socket.

**API**: `/metrics` (no auth, since scrapers can't send an API key) exposes
`taskforge_jobs_by_status` (from a live query), request counts by route/status, and
submissions by task type, via `prometheus_client` on a per-app registry (so creating a
second `FastAPI` instance in the same process, as every test does, doesn't collide on
metric names).

**Grafana**: `monitoring/grafana/dashboards/taskforge.json`, 7 panels (job counts,
throughput, pool utilisation, leases reaped, API request rate, submissions by type,
claim errors), provisioned automatically via `monitoring/grafana/provisioning/`.

**Dashboard** (`dashboard/index.html`): a single self-contained HTML/CSS/JS file --
large tabular job counts, ticket-style rows color-coded by status, a slide-over panel
with attempt history and logs, dead-letter list with one-click requeue, 2-second
polling. Verified with Playwright against the real running stack (not just eyeballed):
confirmed live status text, correct counts matching `/v1/stats`, all 7 seeded jobs
rendered, the dead-letter row present with its real error message, the drawer opening
with the right job's data, status filtering working, and a clean run at a 390px mobile
viewport. This process caught two real bugs before they shipped: a JS typo
(`refresh()` called instead of the actual `refreshAll()` function) and a missing CORS
header that silently blocked every request from the dashboard's origin -- both fixed
and reverified live.

## Docker Compose

`docker-compose.yml` defines 7 core services (postgres, api, scheduler, webhook-worker,
dashboard, prometheus, grafana) plus two optional profiles (`seed` for a demo image,
`ai` for the Phase 8 worker). Every environment variable it sets was cross-checked
against what the code actually reads (`grep` for every `getenv`/`os.getenv` call) rather
than assumed. **This sandbox has no Docker daemon, so `docker compose up` itself was
never executed** -- the YAML is schema-valid (parsed successfully with PyYAML) and the
Dockerfiles follow standard multi-stage patterns, but I could not verify the containers
actually build and run together. Treat this as reviewed-but-unexecuted; if something
doesn't come up cleanly on the first `docker compose up --build`, that's the most likely
place for a mistake to be hiding.

## Load testing

`loadtest/locustfile.py`, run headless against the real live stack (API + C++ scheduler
+ PostgreSQL, all on this sandbox's single shared vCPU) with 30 concurrent users for 60
seconds:

| metric | value |
|---|---|
| total requests | 8,589 |
| job submissions | 3,904 (65/s) |
| aggregate throughput | 143 req/s |
| submission latency | p50 200ms, p95 340ms, p99 390ms |
| failures | 0 |
| queue drain time after load stopped | ~3 seconds |

k6 was not available in this sandbox (no binary download path), so I used Locust
instead, which is pip-installable. These numbers reflect one shared vCPU contending
with PostgreSQL and the scheduler in the same sandbox -- they are a floor, not a
representative production benchmark. Re-run `locust -f loadtest/locustfile.py --host
<url> --headless -u 50 -r 10 -t 60s` on real hardware for numbers you can rely on.

## Phase 8: PyTorch document-classification worker (optional)

`ai_worker/` is a real (if deliberately small) trained model, not a mock: bag-of-words
term-frequency features over a ~240-word vocabulary, a two-layer MLP
(`nn.Linear -> ReLU -> Dropout -> nn.Linear`), trained with real backprop
(`nn.CrossEntropyLoss` + Adam) on a small labelled dataset of five document categories
(invoice, resume, contract, email, other), cached to disk after first training. Verified
standalone: **5/5 correct classifications on held-out text the model never trained on**,
with genuine softmax confidence scores.

The worker (`ai_worker/main.py`) is a second, independent implementation of the same
job-claiming SQL contract as the C++ scheduler (`ai_worker/job_store.py`, a direct port
of `job_store.cpp`'s statements) -- proving the queue's contract lives entirely in SQL
against the `jobs` table, not in a client library, so any language can be a worker.
19 tests, including a concurrency test with a real second `JobStore` connection proving
a simulated "C++ scheduler" and the Python AI worker never claim the same row.

Registered as task type `classify_document` (payload: `{"text": "..."}`, max 20,000
characters) in the same API, subject to the same size limits, validation, and dead-letter
handling as every other task type.

## Known limitations

- **Docker Compose was written and validated (YAML/JSON syntax, env-var names
  cross-checked against source) but never executed** -- no Docker daemon in this
  development sandbox. Everything else in this project was run for real against a live
  PostgreSQL instance; this is the one exception.
- k6 was unavailable in this sandbox; Locust was used instead for load testing (see
  above).
- The Phase 8 classifier is intentionally small (a from-scratch MLP, not a pretrained
  transformer) to keep the demo fast and dependency-light; the worker architecture
  (claim -> classify -> complete, same SQL contract) is what would carry over to a
  larger model.
- Metrics from multiple scheduler replicas scaled with `docker compose up --scale
  scheduler=N` would all try to expose port 9464 on the host; the Compose file as
  written supports one scheduler replica cleanly. Scaling further needs either a
  per-replica port mapping or Prometheus service-discovery instead of the static
  `targets:` list in `monitoring/prometheus.yml`.

## Re-verification (full clean re-run)

Re-ran the entire project from a clean state to confirm it actually works, not just
that it worked once during development: fresh PostgreSQL database (dropped and
recreated from `db/schema.sql`), full clean C++ rebuild (`rm -rf build`, zero warnings),
all 51 C++ tests passing, both sanitizers (Thread/Address) clean, and a completely
fresh live end-to-end run exercising every mechanism at once: priority scheduling, real
image resize, report generation, retry-with-backoff (2 failures then success on attempt
3), permanent-failure dead-lettering, webhook delivery to a real local receiver, and
crash recovery via an actual `kill -9` (not simulated) followed by a second scheduler
process reclaiming and completing the job. Every number and status shown in this
section came from that run, not from memory of the earlier one.

**This caught a real bug**: `api/requirements.txt` and `ai_worker/requirements.txt`
were both missing dependencies (`prometheus_client`, `psycopg_pool`, and `pytest`
respectively) that the code actually imports. The tests had been passing throughout
development because those packages were already installed globally in the sandbox from
earlier work, which masked the gap. Caught by diffing `grep`'d import statements against
each `requirements.txt`, then confirmed fixed by installing into an **actual clean
virtualenv** (`python3 -m venv`, no global packages) and re-running the full API test
suite there -- 78/78 passed from the clean install. This is the kind of bug that looks
fine in a long-lived dev environment and only surfaces on someone else's fresh machine,
which is exactly why this recheck was worth doing.

No other bugs turned up. Both `docker-compose.yml` and the Grafana dashboard JSON
re-validated cleanly. The one limitation from before still holds: no Docker daemon
in this sandbox, so `docker compose up` itself remains unexecuted.

## Cross-check pass (every file against every other file)

A different kind of check than re-running tests: this pass looked specifically for
*inconsistencies between files* -- the sort of bug where each file is individually
correct but two files disagree about a name, path, or port. Tests within one component
can't catch these; they only show up when something tries to actually connect the
pieces.

Checked and confirmed consistent:
- Every `TASKFORGE_*` environment variable set in `docker-compose.yml`, against every
  one actually read by the C++ scheduler, the API, the webhook worker, and the AI
  worker (via `grep` across all four codebases) -- no typos in either direction.
- Every column referenced in SQL across `job_store.cpp`, `main.py`, `webhook_worker.py`,
  and the AI worker's `job_store.py`, against the live database's actual schema
  (`information_schema.columns`, not just the `.sql` file) -- and more importantly,
  all 148 tests execute these exact statements against real PostgreSQL, which would
  hard-fail on any column typo.
- Every task type registered in the API (`tasks.py`) has exactly one handler: 3 in the
  C++ scheduler, 1 (`classify_document`) in the AI worker -- no orphans either way.
- Every metric name the Grafana dashboard queries is actually exported (9 from the C++
  scheduler, 3 from the API) -- no panel pointing at a metric that doesn't exist.
- Prometheus's scrape targets (`scheduler:9464`, `api:8000`) match real Compose service
  names and the ports each process actually listens on.
- Every field the dashboard's JavaScript reads off a job, attempt, log, or dead-letter
  object matches the corresponding Pydantic schema field-for-field.
- Every route the Locust load test calls matches a real, defined API route.
- Every Compose service's resolved environment (after expanding the `x-app-env` YAML
  anchor) was printed and checked by hand -- the anchor merges correctly for every
  service, nothing silently overwritten.

**Two real bugs found and fixed by this pass:**

1. **The dashboard would not have worked out of the box under `docker compose up`.**
   Opened at its own Compose port (8080), the dashboard's browser-side default was to
   query its own origin -- but the API listens on 8000, a different port, and
   `dashboard/nginx.conf` had no route to it. Fixed by adding an nginx reverse-proxy
   rule for `/v1`, `/metrics`, and `/healthz`. That fix then exposed a second, subtler
   problem: a naive `proxy_pass http://api:8000;` is resolved by nginx once, at
   startup, so if nginx starts before `api`'s DNS entry exists (a real race even with
   `depends_on`), nginx fails to start at all. Confirmed this exact failure with a real
   `nginx -t` ("host not found in upstream"), then fixed it with the standard pattern
   (a variable plus Docker's embedded resolver) that defers resolution to request time,
   and confirmed the corrected config passes `nginx -t` without needing the hostname to
   resolve at all.
2. **The `ai-worker` service had no volume mount.** Its trained model
   (`/data/model/classifier.pt`) would have lived only in the container's ephemeral
   filesystem, silently retraining from scratch on every restart instead of persisting
   like every other stateful service in the stack. Fixed by sharing the same
   `taskforge_data` volume the scheduler already uses (different subdirectory, no
   collision).

No other inconsistencies turned up. Both fixes are config/wiring issues specific to the
Docker Compose deployment path -- the one part of this project that still has not been
verified by actually running `docker compose up` (no Docker daemon in this sandbox).
Everything re-run directly (not through Compose) still passes: 51 C++ tests, 78 API
tests, 19 AI worker tests, zero regressions.

## Docker volume permission analysis (a third bug, found by reasoning through Docker semantics rather than by running it)

Since Docker itself isn't runnable in this sandbox, I worked through Docker's documented
volume-population behavior by hand for every place two containers share a volume, rather
than stopping at "the YAML is valid."

**The bug**: `scheduler` and `ai-worker` both mount the same `taskforge_data` volume, but
their Dockerfiles created a `taskforge` user with two *different* UIDs (10001 and 10003).
Docker populates a fresh named volume's ownership from whichever container's image
directory is mounted there first (this is documented Docker behavior, not an edge case).
Whichever of these two services happened to start second would inherit a `/data` owned by
the *other* service's UID with mode `755` -- and the AI worker specifically needs to
`mkdir` a new `/data/model` subdirectory at runtime (to cache its trained model), which a
non-owning, non-root UID cannot do under `755`. Depending on start order, this would have
surfaced as a `PermissionError` the first time the AI worker profile was actually used.

**The fix**: both Dockerfiles now explicitly `groupadd -g 10001` before `useradd -u 10001
-g 10001`, so the UID *and* GID match exactly regardless of which image happened to
initialize the volume first. Verified the exact command sequence (`groupadd`/`useradd`
with these flags) actually produces matching UID and GID by running it for real in this
sandbox (`id dockertest` confirmed `uid=19999(dockertest) gid=19999(dockertest)`) --
`shadow-utils` is the same upstream package on both Ubuntu (the scheduler's base image)
and Debian (the AI worker's `python:3.12-slim` base), so this generalizes across both.

The `seed` service was checked too and needs no fix: it uses a bare `python:3.12-slim`
image with no `USER` directive, so it runs as root, and root bypasses Linux permission
checks entirely (`DAC_OVERRIDE`) -- it can always populate `/data/images` regardless of
what owns the directory.

This is the kind of bug that has nothing to do with the application code being correct or
incorrect -- every test still passes, because tests don't exercise container boundaries.
It only exists at the intersection of "two independently-correct Dockerfiles" and "a
shared volume," which is exactly why a cross-check pass looking for inter-file
inconsistencies was worth doing in addition to re-running the existing test suites.

## Exact-invocation verification

One more gap worth closing: the AI worker and webhook worker had only ever been
exercised through pytest, which manipulates `sys.path` and imports things as modules --
not proof that they work the way Docker actually starts them (`python main.py`,
`python -m app.webhook_worker`, `uvicorn app.asgi:app`, as plain OS-level process
invocations with no test-harness scaffolding). Ran all three exactly as their
Dockerfiles/Compose `command:` overrides specify:

- **`uvicorn app.asgi:app --host 0.0.0.0 --port 8000`** (the API's real CMD, not
  `python -m uvicorn`): started cleanly, answered `/healthz`, shut down gracefully on
  SIGTERM.
- **`python -m app.webhook_worker`** (the webhook-worker service's command override):
  started, claimed a real pending delivery, and delivered a real signed payload to a
  local HTTP receiver -- confirmed via the receiver's own log, not just the worker's
  self-report.
- **`python main.py`** (the ai-worker's real CMD, no `-m`, no sys.path tricks): this
  turned up a genuine, previously-unverified fact rather than a bug -- a cold `import
  torch` took **~16.6 seconds** in this sandbox, almost entirely I/O time reading the
  ~3.2GB of NVIDIA/CUDA shared libraries the default PyPI `torch` wheel bundles, which
  this small CPU-only model never uses. My first two attempts at this test used too
  short a timeout and looked like a hang; a longer one showed the real sequence
  (model loads, heartbeat recorded, both test jobs claimed, classified correctly as
  `invoice` with real confidence scores, and logged) end to end, using the literal
  command Docker will run.

**Fixed as a result**: `ai_worker/Dockerfile` now installs from PyTorch's CPU-only wheel
index first (falling back to the default index if that's unreachable), which avoids
that multi-gigabyte CUDA download and the associated slow cold-start entirely for a
model that never uses a GPU. This sandbox's network allow-list doesn't include
PyTorch's CPU wheel index, so I couldn't re-test the smaller build here directly, but
the fix doesn't change `model.py`'s behavior at all -- only which build of the same
`torch` API gets installed -- and the CUDA build was already proven to work correctly
above, which is the strictly heavier version of the same test.

No further discrepancies found. Full regression after these fixes: 51 C++ tests, 78
API tests, 19 AI worker tests, all passing, zero regressions.

## Clean-room Dockerfile verification (debootstrap chroot, no Docker daemon needed)

The biggest remaining uncertainty was always the same: does `cpp/Dockerfile` actually
build and run, given this sandbox has no Docker daemon? This pass got as close to a
real answer as possible without one: used `debootstrap` to build a genuinely fresh,
minimal Ubuntu 24.04 root filesystem from scratch (not this sandbox's environment,
which has many extra packages already installed that could mask a missing dependency),
`chroot`'d into it, and ran the Dockerfile's exact `apt-get install` lines and build
commands as literal shell commands.

**Build stage** (`g++ cmake make libpq-dev nlohmann-json3-dev ca-certificates`, nothing
else): installed cleanly, `cmake --build build --target taskforge_scheduler` produced a
working binary. This caught a real, non-obvious problem:

**Found and fixed: every Docker build was unnecessarily fetching GoogleTest from
GitHub.** CMake's *configure* step evaluates the entire `CMakeLists.txt` regardless of
which target you later build, so even `cmake --build build --target
taskforge_scheduler` was triggering the `FetchContent_Declare(googletest ...)` fallback
-- confirmed directly: with no `libgtest-dev` installed (the Dockerfile never installs
it), `build/_deps/googletest-src` appeared after configuring, proving the fetch really
happened. This adds needless build time and a hard *network* dependency to every
production image build, for a test framework the shipped binary never uses. Fixed by
gating the entire test section behind a new `TASKFORGE_BUILD_TESTS` CMake option
(default `ON`, so nothing changes for normal development), and passing
`-DTASKFORGE_BUILD_TESTS=OFF` from `cpp/Dockerfile`. Reran the same chroot test after
the fix: configure finished in 0.3 seconds with no `_deps` directory at all, and the
scheduler binary still built and ran correctly. Rebuilt the normal (tests-on) developer
build afterward too, to confirm the option doesn't change anything for everyday use --
all 51 C++ tests still pass.

**Runtime stage** (`libpq5 ca-certificates`, non-root user, nothing else): built a
*second*, separate fresh chroot matching only this minimal package set, copied in the
binary built above, and ran it as the unprivileged `taskforge` user with a real
PostgreSQL DSN pointing back at this machine's actual database over TCP. It connected,
started its embedded metrics server, claimed a real seeded job, ran the handler's
validation logic (correctly dead-lettered it -- my test insert hadn't included a
payload, which is a test-data gap, not a code bug), and shut down cleanly on SIGTERM.
No missing shared library errors, no permission errors running as a non-root user in a
directory it does not own by default.

This is the strongest evidence available in this sandbox that the Docker images are
correctly specified: the exact package lists and exact build/run commands from both
Dockerfile stages, run for real, in environments built from nothing but those package
lists. It is still not identical to `docker build` + `docker run` (no image layers, no
container runtime, no Docker networking) -- so this doesn't replace actually running
Compose -- but it tests the part most likely to silently break (missing a package, a
network dependency creeping into a build that shouldn't need one, a missing shared
library at runtime, a permissions problem for the non-root user) far more rigorously
than reading the Dockerfile ever could.

## Live nginx proxy traffic + clean-room API verification

Two more gaps closed this round.

**Live nginx proxy traffic, not just config validation.** The earlier nginx work
proved the reverse-proxy config was *valid* (`nginx -t`) but a live traffic test had
caused a 300-second tool hang and was abandoned. Root-caused it properly this time:
nginx's own process stdout/stderr weren't redirected away from the invoking shell
(only its *internal* `access_log`/`error_log` directives were), so the daemonized
process kept the tool's output pipe open indefinitely. Fixed by explicitly redirecting
the nginx invocation itself (`</dev/null >log 2>&1 &`) and running the whole test as
one atomic command with an explicit kill at the end. With that fixed, got genuine live
proof: a static file served directly, `GET`/`POST /v1/jobs`, and `/healthz` all
proxied through nginx to a real running API and back, confirmed independently by
nginx's own access log showing all five real requests.

**Clean-room verification of `api/Dockerfile`, including as the non-root user.** Same
`debootstrap` chroot technique as before, this time for the API image. Approximated
`python:3.12-slim` with Ubuntu 24.04's system Python 3.12 (the base image itself isn't
pullable here -- no Docker registry access -- but the actual thing being tested,
whether `pip install -r requirements.txt` succeeds with only `libpq5` present and no
compiler or dev headers, doesn't depend on which distro provides that Python). This
surfaced a real environment quirk, tracked down rather than worked around blindly:
`pip install` failed with a certificate verification error against PyPI. Traced it to
its actual cause -- this sandbox routes all egress through an internal TLS-intercepting
proxy, trusted on the host via custom CA certificates installed outside the normal
package manager, which a from-scratch chroot naturally doesn't have. Confirmed this
directly (`openssl s_client` against pypi.org showed the intercepting proxy's own
issuer certificate) before concluding it was environmental and not a real dependency
problem, then fixed it by copying the same CA certificates into the chroot. With that
resolved: every package installed from prebuilt wheels -- `psycopg[binary]` pulled a
manylinux wheel bundling its own libpq, `pydantic-core` needed no Rust compilation --
confirming the Dockerfile's `libpq5`-only runtime dependency is genuinely sufficient
with no hidden compiler requirement. Then ran the app itself as the actual unprivileged
`taskforge` user against a real PostgreSQL connection over the network: health check
passed, a real job submission returned 201 with a real UUID.

**`ai_worker/Dockerfile`'s CPU-wheel fix: confirmed unreachable in this sandbox
specifically, for a documented reason, not a defect.** Directly tested
`download.pytorch.org` and got an HTTP 403 from the sandbox's own egress gateway --
reachable at the network level, explicitly blocked by policy, not a DNS or code issue.
The Dockerfile's designed fallback (`|| pip install ... default index`) would trigger
here and pull the full CUDA build -- exactly the build already proven to work
end-to-end earlier (the real `python main.py` run that claimed and correctly classified
two real jobs). Did not attempt a redundant full chroot reinstall of a ~5GB torch build
given only 4.4GB of disk headroom in this sandbox; the marginal evidence wasn't worth
the real risk of exhausting disk mid-verification. The CPU-wheel path itself will only
be exercisable on real infrastructure with normal, unfiltered internet access.

Full regression after all of the above: 51 C++ tests, 78 API tests, 19 AI worker tests,
all still passing.
