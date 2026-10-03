// taskforge_scheduler: the worker process. Configuration via environment variables:
//   TASKFORGE_DSN (required) TASKFORGE_CONCURRENCY=4 TASKFORGE_BATCH=50
//   TASKFORGE_LEASE_SECONDS=60 TASKFORGE_POLL_MS=100 TASKFORGE_DATA_DIR=/data TASKFORGE_WORKER_ID
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include "taskforge/handlers.hpp"
#include "taskforge/metrics_server.hpp"
#include "taskforge/scheduler.hpp"

using namespace taskforge;

static std::atomic<bool> g_stop{false};
static void on_signal(int) { g_stop = true; }

static std::string env(const char* k, const std::string& d = "") {
  const char* v = std::getenv(k);
  return (v && *v) ? v : d;
}
static int env_int(const char* k, int d) { auto s = env(k); return s.empty() ? d : std::atoi(s.c_str()); }

int main() {
  SchedulerOptions o;
  o.dsn = env("TASKFORGE_DSN");
  if (o.dsn.empty()) { std::fprintf(stderr, "TASKFORGE_DSN is required\n"); return 2; }
  char host[256] = "host";
  gethostname(host, sizeof host);
  o.hostname = host;
  o.worker_id = env("TASKFORGE_WORKER_ID", std::string(host) + "-" + std::to_string(getpid()));
  o.threads = static_cast<std::size_t>(std::max(1, env_int("TASKFORGE_CONCURRENCY", 4)));
  o.batch = std::max(1, env_int("TASKFORGE_BATCH", 50));
  o.lease_seconds = std::max(5, env_int("TASKFORGE_LEASE_SECONDS", 60));
  o.poll_ms = std::max(10, env_int("TASKFORGE_POLL_MS", 100));
  o.data_dir = env("TASKFORGE_DATA_DIR", "/data");

  struct sigaction sa{};
  sa.sa_handler = on_signal;
  sigaction(SIGINT, &sa, nullptr);
  sigaction(SIGTERM, &sa, nullptr);

  // Wait for PostgreSQL (docker compose starts everything at once).
  for (int i = 0;; ++i) {
    try { db::Connection probe(o.dsn); break; }
    catch (const std::exception& e) {
      if (i >= 60 || g_stop) { std::fprintf(stderr, "database unreachable: %s\n", e.what()); return 1; }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

  Scheduler sched(o);
  register_builtin_handlers(sched);
  sched.start();

  int metrics_port = env_int("TASKFORGE_METRICS_PORT", 9464);
  std::unique_ptr<MetricsServer> metrics;
  auto render = [&sched, &o]() -> std::string {
    auto c = sched.counters();
    auto p = sched.pool_stats();
    auto g = [](const char* name, const char* help, const char* type) {
      return std::string("# HELP ") + name + " " + help + "\n# TYPE " + name + " " + type + "\n";
    };
    std::string out;
    out += g("taskforge_jobs_claimed_total", "Jobs claimed from the queue by this worker", "counter");
    out += "taskforge_jobs_claimed_total{worker=\"" + o.worker_id + "\"} " + std::to_string(c.claimed) + "\n";
    out += g("taskforge_jobs_succeeded_total", "Jobs completed successfully", "counter");
    out += "taskforge_jobs_succeeded_total{worker=\"" + o.worker_id + "\"} " + std::to_string(c.succeeded) + "\n";
    out += g("taskforge_jobs_failed_total", "Job execution attempts that raised an error", "counter");
    out += "taskforge_jobs_failed_total{worker=\"" + o.worker_id + "\"} " + std::to_string(c.failed) + "\n";
    out += g("taskforge_jobs_retried_total", "Failed attempts rescheduled with backoff", "counter");
    out += "taskforge_jobs_retried_total{worker=\"" + o.worker_id + "\"} " + std::to_string(c.retried) + "\n";
    out += g("taskforge_jobs_dead_total", "Jobs moved to the dead letter table by this worker", "counter");
    out += "taskforge_jobs_dead_total{worker=\"" + o.worker_id + "\"} " + std::to_string(c.dead) + "\n";
    out += g("taskforge_leases_reaped_total", "Crashed jobs recovered via expired lease", "counter");
    out += "taskforge_leases_reaped_total{worker=\"" + o.worker_id + "\"} " + std::to_string(c.leases_reaped) + "\n";
    out += g("taskforge_claim_errors_total", "Database errors while claiming jobs", "counter");
    out += "taskforge_claim_errors_total{worker=\"" + o.worker_id + "\"} " + std::to_string(c.claim_errors) + "\n";
    out += g("taskforge_pool_threads", "Configured worker threads", "gauge");
    out += "taskforge_pool_threads{worker=\"" + o.worker_id + "\"} " + std::to_string(p.threads) + "\n";
    out += g("taskforge_pool_active", "Jobs currently executing", "gauge");
    out += "taskforge_pool_active{worker=\"" + o.worker_id + "\"} " + std::to_string(p.active) + "\n";
    out += g("taskforge_pool_queued", "Claimed jobs waiting for a free thread", "gauge");
    out += "taskforge_pool_queued{worker=\"" + o.worker_id + "\"} " + std::to_string(p.queued) + "\n";
    return out;
  };
  try {
    metrics = std::make_unique<MetricsServer>(metrics_port, render);
    std::fprintf(stderr, "[%s] metrics on :%d/metrics\n", o.worker_id.c_str(), metrics_port);
  } catch (const std::exception& e) {
    std::fprintf(stderr, "[%s] metrics server disabled: %s\n", o.worker_id.c_str(), e.what());
  }

  std::fprintf(stderr, "[%s] started: threads=%zu batch=%d lease=%ds poll=%dms data_dir=%s\n", o.worker_id.c_str(),
               o.threads, o.batch, o.lease_seconds, o.poll_ms, o.data_dir.c_str());
  while (!g_stop) std::this_thread::sleep_for(std::chrono::milliseconds(100));
  std::fprintf(stderr, "[%s] shutting down: finishing running jobs...\n", o.worker_id.c_str());
  sched.stop();
  auto c = sched.counters();
  std::fprintf(stderr, "[%s] stopped: claimed=%llu succeeded=%llu failed=%llu\n", o.worker_id.c_str(),
               (unsigned long long)c.claimed, (unsigned long long)c.succeeded, (unsigned long long)c.failed);
  return 0;
}
