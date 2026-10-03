// Scheduler: the loop that turns durable rows into running work.
//
//   loop:  n = pool.free_slots()
//          jobs = JobStore::claim(worker, min(n, batch), lease)   // atomic, committed
//          for each job: pool.try_post(priority, run_job)
//          wait until a job finishes (wake) or poll_ms elapses
//
// Claiming only as many jobs as there are free worker slots means (a) priority is
// decided in PostgreSQL at the last possible moment, so a high-priority job that
// arrives while all threads are busy is picked before older low-priority ones,
// and (b) a job's lease never ticks away while it waits in an in-memory queue.
#pragma once
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "taskforge/handler.hpp"
#include "taskforge/job_store.hpp"
#include "taskforge/thread_pool.hpp"

namespace taskforge {

struct SchedulerOptions {
  std::string dsn;
  std::string worker_id;      // must be unique per scheduler process
  std::string hostname;       // for the workers table / dashboard
  std::size_t threads = 4;    // max concurrent jobs
  int batch = 50;             // max jobs per claim query
  int lease_seconds = 60;
  int poll_ms = 100;          // idle polling interval
  std::string data_dir = "/data";
  double retry_base_delay_secs = 2.0;    // exponential backoff base
  double retry_max_delay_secs = 300.0;   // backoff cap
  int heartbeat_ms = 5000;               // how often to update the workers table
  int reap_interval_ms = 5000;           // how often to scan for expired leases
};

struct SchedulerCounters {
  std::uint64_t claimed = 0, succeeded = 0, failed = 0, retried = 0, dead = 0,
      lease_lost = 0, claim_errors = 0, leases_reaped = 0;
};

class Scheduler {
 public:
  explicit Scheduler(SchedulerOptions opts);
  ~Scheduler();
  Scheduler(const Scheduler&) = delete;
  Scheduler& operator=(const Scheduler&) = delete;

  void register_handler(const std::string& task_type, Handler h);  // before start()
  void start();
  // Stop claiming; let running jobs finish; join everything. Idempotent.
  void stop();

  SchedulerCounters counters() const;
  const std::string& worker_id() const { return opt_.worker_id; }
  PoolStats pool_stats() const { return pool_->stats(); }

 private:
  void loop();
  void maintenance_loop();  // heartbeat + lease reaper, separate thread/connection
  void run_job(const ClaimedJob& job);
  void wake();

  SchedulerOptions opt_;
  std::unordered_map<std::string, Handler> handlers_;  // immutable after start()
  std::unique_ptr<ThreadPool> pool_;
  std::thread loop_thread_;
  std::thread maintenance_thread_;
  std::mutex mu_;
  std::condition_variable cv_;
  bool stop_ = false, wake_ = false, started_ = false;
  std::atomic<std::uint64_t> claimed_{0}, succeeded_{0}, failed_{0}, retried_{0}, dead_{0},
      lease_lost_{0}, claim_errors_{0}, leases_reaped_{0};
};

}  // namespace taskforge
