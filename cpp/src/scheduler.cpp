#include "taskforge/scheduler.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <ctime>

namespace taskforge {
namespace {

void log_line(const std::string& worker, const std::string& msg) {
  std::time_t t = std::time(nullptr);
  char buf[32];
  std::strftime(buf, sizeof buf, "%H:%M:%S", std::gmtime(&t));
  std::fprintf(stderr, "[%s] [%s] %s\n", buf, worker.c_str(), msg.c_str());
}

// One connection per pool thread, created lazily and reused for every job that
// thread runs (libpq connections are not thread-safe, so they are never shared).
db::Connection& worker_conn(const std::string& dsn) {
  thread_local std::unique_ptr<db::Connection> conn;
  if (!conn) conn = std::make_unique<db::Connection>(dsn);
  return *conn;
}

}  // namespace

Scheduler::Scheduler(SchedulerOptions opts) : opt_(std::move(opts)) {
  PoolOptions po;
  po.threads = std::max<std::size_t>(1, opt_.threads);
  pool_ = std::make_unique<ThreadPool>(po);
}

Scheduler::~Scheduler() { stop(); }

void Scheduler::register_handler(const std::string& task_type, Handler h) {
  handlers_[task_type] = std::move(h);
}

void Scheduler::start() {
  std::lock_guard<std::mutex> lk(mu_);
  if (started_) return;
  started_ = true;
  loop_thread_ = std::thread([this] { loop(); });
  maintenance_thread_ = std::thread([this] { maintenance_loop(); });
}

void Scheduler::stop() {
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (!started_ && !pool_) return;
    stop_ = true;
  }
  cv_.notify_all();
  if (loop_thread_.joinable()) loop_thread_.join();
  if (maintenance_thread_.joinable()) maintenance_thread_.join();
  pool_->shutdown(/*drain=*/true);  // running (and any queued) jobs finish
}

void Scheduler::wake() {
  { std::lock_guard<std::mutex> lk(mu_); wake_ = true; }
  cv_.notify_one();
}

SchedulerCounters Scheduler::counters() const {
  return {claimed_.load(), succeeded_.load(), failed_.load(), retried_.load(), dead_.load(),
          lease_lost_.load(), claim_errors_.load(), leases_reaped_.load()};
}

// Runs on its own connection so a slow claim/dispatch cycle never delays heartbeats,
// and a stalled maintenance connection never blocks job dispatch.
void Scheduler::maintenance_loop() {
  std::unique_ptr<db::Connection> conn;
  auto next_heartbeat = std::chrono::steady_clock::now();
  auto next_reap = next_heartbeat;
  for (;;) {
    {
      std::unique_lock<std::mutex> lk(mu_);
      if (stop_) return;
    }
    auto now = std::chrono::steady_clock::now();
    try {
      if (!conn) conn = std::make_unique<db::Connection>(opt_.dsn);
      JobStore store(*conn);
      if (now >= next_heartbeat) {
        store.heartbeat(opt_.worker_id, opt_.hostname, static_cast<int>(opt_.threads));
        next_heartbeat = now + std::chrono::milliseconds(opt_.heartbeat_ms);
      }
      if (now >= next_reap) {
        int n = store.reap_expired_leases(opt_.retry_base_delay_secs, opt_.retry_max_delay_secs);
        if (n > 0) {
          leases_reaped_ += n;
          log_line(opt_.worker_id, "reaped " + std::to_string(n) + " job(s) with an expired lease");
          wake();  // a scheduler (possibly this one) can now claim the recovered jobs
        }
        next_reap = now + std::chrono::milliseconds(opt_.reap_interval_ms);
      }
    } catch (const std::exception& e) {
      conn.reset();
      log_line(opt_.worker_id, std::string("maintenance loop error: ") + e.what());
    }
    std::unique_lock<std::mutex> lk(mu_);
    if (stop_) return;
    cv_.wait_for(lk, std::chrono::milliseconds(250), [this] { return stop_; });
  }
}

void Scheduler::loop() {
  std::unique_ptr<db::Connection> conn;
  int backoff_ms = 0;
  for (;;) {
    {
      std::unique_lock<std::mutex> lk(mu_);
      if (stop_) return;
    }
    bool claimed_any = false;
    try {
      if (!conn) conn = std::make_unique<db::Connection>(opt_.dsn);
      std::size_t free = pool_->free_slots();
      if (free > 0) {
        JobStore store(*conn);
        int n = static_cast<int>(std::min<std::size_t>(free, static_cast<std::size_t>(opt_.batch)));
        auto jobs = store.claim(opt_.worker_id, n, opt_.lease_seconds);  // committed on return
        claimed_ += jobs.size();
        claimed_any = !jobs.empty();
        for (auto& j : jobs) {
          // Post by priority; if the pool refuses (shutting down) the lease will expire
          // and the job is recovered by another worker.
          if (!pool_->try_post(j.priority, [this, j] { run_job(j); })) {
            log_line(opt_.worker_id, "pool refused job " + j.id + "; it will be recovered after lease expiry");
          }
        }
      }
      backoff_ms = 0;
    } catch (const std::exception& e) {
      ++claim_errors_;
      conn.reset();
      backoff_ms = std::min(backoff_ms == 0 ? 200 : backoff_ms * 2, 5000);
      log_line(opt_.worker_id, std::string("claim failed: ") + e.what());
    }

    std::unique_lock<std::mutex> lk(mu_);
    if (stop_) return;
    // If we just filled every free slot there is nothing to do until a job finishes.
    // If we found work, re-check right away for more; otherwise sleep poll_ms.
    auto wait = std::chrono::milliseconds(backoff_ms ? backoff_ms : opt_.poll_ms);
    if (claimed_any && pool_->free_slots() > 0) wait = std::chrono::milliseconds(0);
    cv_.wait_for(lk, wait, [this] { return stop_ || wake_; });
    wake_ = false;
  }
}

void Scheduler::run_job(const ClaimedJob& job) {
  struct WakeOnExit {  // a slot just freed up: claim the next job immediately
    Scheduler* s;
    ~WakeOnExit() { s->wake(); }
  } wake_guard{this};

  try {
    db::Connection& conn = worker_conn(opt_.dsn);
    JobStore store(conn);

    JobContext ctx;
    ctx.job_id = job.id;
    ctx.attempt = job.attempt;
    ctx.max_retries = job.max_retries;
    ctx.worker_id = opt_.worker_id;
    ctx.dsn = opt_.dsn;
    ctx.data_dir = opt_.data_dir;
    ctx.log = [&store, &job](const std::string& level, const std::string& msg) {
      try { store.add_log(job.id, job.attempt, level, msg); } catch (...) {}  // logging must never fail a job
    };

    std::string error, reason;
    nlohmann::json result;
    try {
      auto it = handlers_.find(job.task_type);
      if (it == handlers_.end()) throw PermanentError("no handler registered for task type '" + job.task_type + "'");
      nlohmann::json payload = nlohmann::json::parse(job.payload_json);
      result = it->second(ctx, payload);
    } catch (const PermanentError& e) {
      error = e.what(); reason = "permanent_error";
    } catch (const nlohmann::json::exception& e) {
      error = std::string("invalid payload: ") + e.what(); reason = "permanent_error";
    } catch (const TemporaryError& e) {
      error = e.what(); reason = "handler_error";
    } catch (const std::exception& e) {
      error = e.what(); reason = "handler_error";
    } catch (...) {
      error = "unknown exception"; reason = "handler_error";
    }

    if (error.empty()) {
      if (store.complete(job.id, opt_.worker_id, job.attempt, result.dump())) {
        ++succeeded_;
      } else {
        ++lease_lost_;  // someone else owns this attempt now; our result is discarded
        log_line(opt_.worker_id, "lost ownership of job " + job.id + " before completing it");
      }
    } else {
      ctx.log("error", error);
      ++failed_;
      bool owned;
      if (reason == "permanent_error") {
        // Never worth retrying: bad input, unknown task type, HTTP 400-class errors.
        owned = store.fail_permanent(job.id, opt_.worker_id, job.attempt, error, reason);
        if (owned) ++dead_;
      } else {
        // Recoverable: retry_or_dead consults max_retries and applies exponential
        // backoff, or dead-letters once retries are exhausted.
        auto outcome = store.retry_or_dead(job.id, opt_.worker_id, job.attempt, error,
                                           opt_.retry_base_delay_secs, opt_.retry_max_delay_secs);
        owned = outcome != JobStore::RetryOutcome::NotOwned;
        if (outcome == JobStore::RetryOutcome::Retried) ++retried_;
        else if (outcome == JobStore::RetryOutcome::Dead) ++dead_;
      }
      if (!owned) ++lease_lost_;
    }
  } catch (const std::exception& e) {
    // Database trouble while reporting the outcome. The row stays RUNNING and the
    // lease-expiry recovery (Phase 6) makes another worker pick it up.
    log_line(opt_.worker_id, "could not record outcome of " + job.id + ": " + e.what());
  }
}

}  // namespace taskforge
