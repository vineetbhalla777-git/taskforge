#include "taskforge/job_store.hpp"

#include <algorithm>

namespace taskforge {

std::vector<ClaimedJob> JobStore::claim(const std::string& worker_id, int limit, int lease_seconds) {
  std::vector<ClaimedJob> out;
  if (limit <= 0) return out;

  static const char* kClaimSql = R"SQL(
WITH picked AS (
  SELECT id
    FROM jobs
   WHERE status IN ('queued','retrying')
     AND run_at <= now()
   ORDER BY priority DESC, run_at ASC, created_at ASC
   LIMIT $1::int
     FOR UPDATE SKIP LOCKED
), upd AS (
  UPDATE jobs j
     SET status           = 'running',
         worker_id        = $2,
         attempts         = j.attempts + 1,
         lease_expires_at = now() + make_interval(secs => $3::int),
         started_at       = now(),
         updated_at       = now()
    FROM picked
   WHERE j.id = picked.id
  RETURNING j.id, j.task_type, j.payload::text AS payload, j.priority, j.attempts, j.max_retries
), hist AS (
  INSERT INTO job_attempts (job_id, attempt, worker_id)
  SELECT id, attempts, $2 FROM upd
)
SELECT id::text, task_type, payload, priority, attempts, max_retries FROM upd
)SQL";

  auto r = conn_.exec(kClaimSql, {std::to_string(limit), worker_id, std::to_string(lease_seconds)});
  out.reserve(r.rows());
  for (int i = 0; i < r.rows(); ++i) {
    ClaimedJob j;
    j.id = r.str(i, 0);
    j.task_type = r.str(i, 1);
    j.payload_json = r.str(i, 2);
    j.priority = r.integer(i, 3);
    j.attempt = r.integer(i, 4);
    j.max_retries = r.integer(i, 5);
    out.push_back(std::move(j));
  }
  // UPDATE ... RETURNING does not preserve the CTE's ORDER BY, so restore
  // dispatch order here (stable => keeps DB order among equal priorities).
  std::stable_sort(out.begin(), out.end(),
                   [](const ClaimedJob& a, const ClaimedJob& b) { return a.priority > b.priority; });
  return out;
}

bool JobStore::complete(const std::string& job_id, const std::string& worker_id, int attempt,
                        const std::string& result_json) {
  static const char* kSql = R"SQL(
WITH upd AS (
  UPDATE jobs
     SET status = 'succeeded', result = $4::jsonb, finished_at = now(),
         lease_expires_at = NULL, last_error = NULL, updated_at = now()
   WHERE id = $1::uuid AND worker_id = $2 AND attempts = $3::int AND status = 'running'
  RETURNING id
)
UPDATE job_attempts a
   SET finished_at = now(), outcome = 'succeeded',
       duration_ms = (EXTRACT(EPOCH FROM (now() - a.started_at)) * 1000)::int
  FROM upd
 WHERE a.job_id = upd.id AND a.attempt = $3::int
)SQL";
  auto r = conn_.exec(kSql, {job_id, worker_id, std::to_string(attempt), result_json});
  return r.affected() > 0;
}

bool JobStore::fail_permanent(const std::string& job_id, const std::string& worker_id, int attempt,
                             const std::string& error, const std::string& reason) {
  static const char* kSql = R"SQL(
WITH upd AS (
  UPDATE jobs
     SET status = 'dead', last_error = $4::text, finished_at = now(),
         lease_expires_at = NULL, updated_at = now()
   WHERE id = $1::uuid AND worker_id = $2 AND attempts = $3::int AND status = 'running'
  RETURNING id, attempts, payload
), dl AS (
  INSERT INTO dead_letters (job_id, reason, attempts, last_error, payload)
  SELECT id, $5::text, attempts, $4::text, payload FROM upd
  ON CONFLICT (job_id) DO UPDATE
     SET moved_at = now(), reason = EXCLUDED.reason, attempts = EXCLUDED.attempts,
         last_error = EXCLUDED.last_error
)
UPDATE job_attempts a
   SET finished_at = now(), outcome = 'failed', error = $4::text,
       duration_ms = (EXTRACT(EPOCH FROM (now() - a.started_at)) * 1000)::int
  FROM upd
 WHERE a.job_id = upd.id AND a.attempt = $3::int
)SQL";
  auto r = conn_.exec(kSql, {job_id, worker_id, std::to_string(attempt), error.substr(0, 4000), reason});
  return r.affected() > 0;
}

void JobStore::add_log(const std::string& job_id, int attempt, const std::string& level,
                       const std::string& message) {
  conn_.exec("INSERT INTO job_logs (job_id, attempt, level, message) VALUES ($1::uuid, $2::int, $3, $4)",
             {job_id, std::to_string(attempt), level, message.substr(0, 4000)});
}

JobStore::RetryOutcome JobStore::retry_or_dead(const std::string& job_id, const std::string& worker_id, int attempt,
                            const std::string& error, double base_delay_secs, double max_delay_secs) {
  // One statement decides retry-vs-dead using the row's own max_retries, so the policy
  // cannot race against a concurrent read of max_retries done in C++.
  static const char* kSql = R"SQL(
WITH target AS (
  SELECT id, max_retries, attempts,
         least($5::float8, $4::float8 * power(2, greatest(attempts - 1, 0))) * (0.5 + random() * 0.5) AS delay_secs
    FROM jobs
   WHERE id = $1::uuid AND worker_id = $2 AND attempts = $3::int AND status = 'running'
   FOR UPDATE
), retried AS (
  UPDATE jobs j SET status = 'retrying', last_error = $6::text, worker_id = NULL,
                    lease_expires_at = NULL, run_at = now() + make_interval(secs => t.delay_secs),
                    updated_at = now()
    FROM target t WHERE j.id = t.id AND t.attempts <= t.max_retries
  RETURNING j.id
), dead AS (
  UPDATE jobs j SET status = 'dead', last_error = $6::text, finished_at = now(),
                    lease_expires_at = NULL, updated_at = now()
    FROM target t WHERE j.id = t.id AND t.attempts > t.max_retries
  RETURNING j.id, j.attempts, j.payload
), dl AS (
  INSERT INTO dead_letters (job_id, reason, attempts, last_error, payload)
  SELECT id, 'retries_exhausted', attempts, $6::text, payload FROM dead
  ON CONFLICT (job_id) DO UPDATE SET moved_at = now(), reason = EXCLUDED.reason,
                                     attempts = EXCLUDED.attempts, last_error = EXCLUDED.last_error
), hist AS (
  UPDATE job_attempts a
     SET finished_at = now(), error = $6::text, outcome = 'failed',
         duration_ms = (EXTRACT(EPOCH FROM (now() - a.started_at)) * 1000)::int
    FROM target t WHERE a.job_id = t.id AND a.attempt = $3::int
)
SELECT (SELECT count(*) FROM retried) AS n_retried, (SELECT count(*) FROM dead) AS n_dead
)SQL";
  auto r = conn_.exec(kSql, {job_id, worker_id, std::to_string(attempt), std::to_string(base_delay_secs),
                            std::to_string(max_delay_secs), error.substr(0, 4000)});
  if (r.integer(0, 0) > 0) return RetryOutcome::Retried;
  if (r.integer(0, 1) > 0) return RetryOutcome::Dead;
  return RetryOutcome::NotOwned;
}

int JobStore::reap_expired_leases(double base_delay_secs, double max_delay_secs) {
  // Find candidates first (outside FOR UPDATE, cheap), then run the same fenced
  // retry_or_dead() used by normal failures for each one. A job already completed
  // or reclaimed by someone else between the two steps is simply a no-op (fenced
  // by worker_id+attempt), so this never double-processes a job.
  auto candidates = conn_.exec(
      "SELECT id::text, worker_id, attempts FROM jobs "
      "WHERE status = 'running' AND lease_expires_at < now()");
  int n = 0;
  for (int i = 0; i < candidates.rows(); ++i) {
    std::string id = candidates.str(i, 0), worker = candidates.str(i, 1);
    int attempt = candidates.integer(i, 2);
    auto outcome = retry_or_dead(id, worker, attempt, "lease expired: worker crashed or was killed before finishing",
                                 base_delay_secs, max_delay_secs);
    if (outcome != RetryOutcome::NotOwned) {
      ++n;
      // Distinguish this from an ordinary handler failure in the attempt history.
      conn_.exec("UPDATE job_attempts SET outcome = 'lease_expired' WHERE job_id = $1::uuid AND attempt = $2::int "
                "AND outcome = 'failed' AND finished_at >= now() - interval '5 seconds'", {id, std::to_string(attempt)});
    }
  }
  return n;
}

void JobStore::heartbeat(const std::string& worker_id, const std::string& hostname, int concurrency) {
  conn_.exec(
      "INSERT INTO workers (worker_id, hostname, concurrency, last_heartbeat) VALUES ($1, $2, $3::int, now()) "
      "ON CONFLICT (worker_id) DO UPDATE SET hostname = EXCLUDED.hostname, concurrency = EXCLUDED.concurrency, "
      "last_heartbeat = now()",
      {worker_id, hostname, std::to_string(concurrency)});
}

}  // namespace taskforge
