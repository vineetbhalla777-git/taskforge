// TaskForge Phase 1 - priority thread pool.
//
// Properties:
//   * Fixed number of worker threads, created up front.
//   * Tasks are ordered by priority (HIGHER number runs first); tasks with the
//     same priority run in FIFO order (a monotonically increasing sequence
//     number breaks ties, so ordering is deterministic).
//   * Optional bounded queue: try_post() refuses work instead of growing
//     without limit (back-pressure / resource limit).
//   * submit() returns a std::future; exceptions thrown by a task are delivered
//     through the future and counted in stats().failed.
//   * shutdown(drain=true) finishes queued work; shutdown(drain=false) discards
//     queued work (their futures throw std::future_error/broken_promise).
//   * free_slots() lets the scheduler claim only as many jobs from PostgreSQL
//     as the pool can accept right now (Phase 2).
#pragma once

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace taskforge {

class PoolStoppedError : public std::runtime_error {
 public:
  PoolStoppedError() : std::runtime_error("thread pool is stopped") {}
};

class QueueFullError : public std::runtime_error {
 public:
  QueueFullError() : std::runtime_error("thread pool queue is full") {}
};

struct PoolStats {
  std::size_t threads = 0;
  std::size_t queued = 0;     // waiting in the queue
  std::size_t active = 0;     // currently executing
  std::uint64_t submitted = 0;
  std::uint64_t completed = 0;  // finished without throwing
  std::uint64_t failed = 0;     // finished by throwing
  std::uint64_t rejected = 0;   // refused by try_post (queue full / stopped)
};

struct PoolOptions {
  std::size_t threads = 0;    // 0 => hardware_concurrency (min 1)
  std::size_t max_queue = 0;  // 0 => unbounded
};

class ThreadPool {
 public:
  using Options = PoolOptions;
  explicit ThreadPool(PoolOptions opts = PoolOptions{});
  ~ThreadPool();

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  // Enqueue a callable and get its result through a future.
  // Throws PoolStoppedError / QueueFullError if the task cannot be accepted.
  template <class F>
  auto submit(int priority, F&& f) -> std::future<std::invoke_result_t<std::decay_t<F>>> {
    using R = std::invoke_result_t<std::decay_t<F>>;
    auto promise = std::make_shared<std::promise<R>>();
    auto fut = promise->get_future();
    Job job;
    job.fn = [promise, fn = std::forward<F>(f)]() mutable {
      try {
        if constexpr (std::is_void_v<R>) {
          fn();
          promise->set_value();
        } else {
          promise->set_value(fn());
        }
      } catch (...) {
        promise->set_exception(std::current_exception());
        throw;  // let the worker loop count it as failed
      }
    };
    enqueue(priority, std::move(job), /*throw_on_reject=*/true);
    return fut;
  }

  // Fire-and-forget. Returns false (and counts a rejection) if the pool is
  // stopped or the bounded queue is full. Exceptions from `fn` are swallowed
  // and counted in stats().failed.
  bool try_post(int priority, std::function<void()> fn);

  // Block until the queue is empty and no task is running.
  void wait_idle();

  // Stop accepting work and join all threads. Idempotent.
  void shutdown(bool drain = true);

  PoolStats stats() const;

  // How many more tasks can be accepted without blocking a thread AND without
  // hitting max_queue: (threads - active - queued), never negative. This is
  // the number the scheduler should claim from the database.
  std::size_t free_slots() const;

  std::size_t thread_count() const { return n_threads_; }

 private:
  struct Job {
    int priority = 0;
    std::uint64_t seq = 0;
    std::function<void()> fn;
  };
  // Max-heap: higher priority first, then lower sequence number (FIFO).
  static bool heap_less(const Job& a, const Job& b) {
    if (a.priority != b.priority) return a.priority < b.priority;
    return a.seq > b.seq;
  }

  bool enqueue(int priority, Job&& job, bool throw_on_reject);
  void worker_loop();

  mutable std::mutex mu_;
  std::condition_variable work_cv_;
  std::condition_variable idle_cv_;
  std::vector<Job> heap_;
  std::vector<std::thread> threads_;
  std::size_t n_threads_ = 0;
  std::size_t max_queue_;
  std::size_t active_ = 0;
  std::uint64_t next_seq_ = 0;
  bool stop_ = false;

  std::uint64_t submitted_ = 0, completed_ = 0, failed_ = 0, rejected_ = 0;
};

}  // namespace taskforge
