// JobStore: every state transition of a job lives here, as a single SQL
// statement. Single statements are atomic in PostgreSQL, so the store needs no
// client-side locking and cannot leave a job half-updated.
#pragma once
#include <string>
#include <vector>

#include "taskforge/db.hpp"
#include <vector>

namespace taskforge {

struct ClaimedJob {
  std::string id;
  std::string task_type;
  std::string payload_json;
  int priority = 0;
  int attempt = 0;      // 1-based; equals jobs.attempts after the claim
  int max_retries = 0;
};

class JobStore {
 public:
  explicit JobStore(db::Connection& conn) : conn_(conn) {}

  // Atomically claim up to `limit` runnable jobs (status queued/retrying and
  // run_at <= now), highest priority first, oldest first within a priority.
  //
  // In ONE statement this: selects candidates FOR UPDATE SKIP LOCKED (rows
  // locked by another scheduler are skipped instead of waited on), flips them
  // to RUNNING with worker_id + lease expiry, increments attempts and writes
  // the job_attempts history row. The statement auto-commits, so the claim is
  // durable before any handler runs.
  std::vector<ClaimedJob> claim(const std::string& worker_id, int limit, int lease_seconds);

  // Mark a RUNNING job succeeded. Fenced: only applies if this worker still
  // owns this attempt, so a worker whose lease expired (and whose job was
  // re-claimed elsewhere) cannot overwrite the newer attempt. Returns false
  // if the update was rejected.
  bool complete(const std::string& job_id, const std::string& worker_id, int attempt,
                const std::string& result_json);

  // Move a RUNNING job to `dead` and write its dead_letters record (fenced like
  // complete()). Used directly for PermanentError (retrying can never help) and
  // internally by retry_or_dead() once retries are exhausted.
  bool fail_permanent(const std::string& job_id, const std::string& worker_id, int attempt,
                      const std::string& error, const std::string& reason);

  // A recoverable failure: if `attempt < max_retries` (max_retries is the value stored on
  // the job row), reschedule with exponential backoff + jitter
  //   delay = min(max_delay, base * 2^(attempt-1)) * U(0.5, 1.0)
  // and status 'retrying'; otherwise dead-letter with reason 'retries_exhausted'.
  // Fenced by worker_id + attempt exactly like complete()/fail_permanent().
  enum class RetryOutcome { NotOwned, Retried, Dead };
  // NotOwned if some other worker already owns a newer attempt (this attempt's outcome
  // is then simply discarded); otherwise Retried (rescheduled with backoff) or Dead
  // (retries exhausted, moved to dead_letters), decided from the row's own max_retries.
  RetryOutcome retry_or_dead(const std::string& job_id, const std::string& worker_id, int attempt,
                            const std::string& error, double base_delay_secs = 2.0, double max_delay_secs = 300.0);

  // Append a line to job_logs (visible via GET /v1/jobs/{id}/logs).
  void add_log(const std::string& job_id, int attempt, const std::string& level, const std::string& message);

  // Phase 6: crash recovery + observability -------------------------------------
  // Reclaim jobs whose lease expired (their worker crashed or was killed without
  // finishing). Treated exactly like a handler failure for retry accounting: this
  // consumes one of the job's retries via retry_or_dead so a job that crashes
  // repeatedly still eventually reaches the dead letter table instead of looping
  // forever. Returns how many jobs were reclaimed (retried or dead-lettered).
  int reap_expired_leases(double base_delay_secs = 2.0, double max_delay_secs = 300.0);

  // Upsert this worker's heartbeat row (workers table), used by the dashboard/metrics
  // to show which workers are alive and their configured concurrency.
  void heartbeat(const std::string& worker_id, const std::string& hostname, int concurrency);

 private:
  db::Connection& conn_;
};

}  // namespace taskforge
