#include "taskforge/thread_pool.hpp"

namespace taskforge {

ThreadPool::ThreadPool(PoolOptions opts) : max_queue_(opts.max_queue) {
  std::size_t n = opts.threads;
  if (n == 0) n = std::max<std::size_t>(1, std::thread::hardware_concurrency());
  n_threads_ = n;
  threads_.reserve(n);
  for (std::size_t i = 0; i < n; ++i) {
    threads_.emplace_back([this] { worker_loop(); });
  }
}

ThreadPool::~ThreadPool() { shutdown(true); }

bool ThreadPool::enqueue(int priority, Job&& job, bool throw_on_reject) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (stop_) {
      ++rejected_;
      if (throw_on_reject) throw PoolStoppedError();
      return false;
    }
    if (max_queue_ != 0 && heap_.size() >= max_queue_) {
      ++rejected_;
      if (throw_on_reject) throw QueueFullError();
      return false;
    }
    job.priority = priority;
    job.seq = next_seq_++;
    heap_.push_back(std::move(job));
    std::push_heap(heap_.begin(), heap_.end(), heap_less);
    ++submitted_;
  }
  work_cv_.notify_one();
  return true;
}

bool ThreadPool::try_post(int priority, std::function<void()> fn) {
  Job job;
  job.fn = std::move(fn);
  return enqueue(priority, std::move(job), /*throw_on_reject=*/false);
}

void ThreadPool::worker_loop() {
  for (;;) {
    Job job;
    {
      std::unique_lock<std::mutex> lk(mu_);
      work_cv_.wait(lk, [this] { return stop_ || !heap_.empty(); });
      if (heap_.empty()) return;  // stop_ is set and nothing left to do
      std::pop_heap(heap_.begin(), heap_.end(), heap_less);
      job = std::move(heap_.back());
      heap_.pop_back();
      ++active_;
    }

    bool ok = true;
    try {
      job.fn();
    } catch (...) {
      ok = false;
    }
    job.fn = nullptr;  // release captured state before reporting idle

    {
      std::lock_guard<std::mutex> lk(mu_);
      --active_;
      ok ? ++completed_ : ++failed_;
      if (heap_.empty() && active_ == 0) idle_cv_.notify_all();
    }
  }
}

void ThreadPool::wait_idle() {
  std::unique_lock<std::mutex> lk(mu_);
  idle_cv_.wait(lk, [this] { return heap_.empty() && active_ == 0; });
}

void ThreadPool::shutdown(bool drain) {
  std::vector<Job> discarded;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (!stop_) {
      stop_ = true;
      if (!drain) discarded.swap(heap_);  // destroyed below, outside the lock
    }
  }
  work_cv_.notify_all();
  discarded.clear();  // destroying promises => waiting futures get broken_promise

  std::vector<std::thread> to_join;
  {
    std::lock_guard<std::mutex> lk(mu_);
    to_join.swap(threads_);
  }
  for (auto& t : to_join) {
    if (t.joinable() && t.get_id() != std::this_thread::get_id()) t.join();
  }
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (heap_.empty() && active_ == 0) idle_cv_.notify_all();
  }
}

PoolStats ThreadPool::stats() const {
  std::lock_guard<std::mutex> lk(mu_);
  PoolStats s;
  s.threads = n_threads_;
  s.queued = heap_.size();
  s.active = active_;
  s.submitted = submitted_;
  s.completed = completed_;
  s.failed = failed_;
  s.rejected = rejected_;
  return s;
}

std::size_t ThreadPool::free_slots() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::size_t busy = active_ + heap_.size();
  std::size_t n = stop_ ? 0 : n_threads_;
  std::size_t by_threads = busy >= n ? 0 : n - busy;
  if (max_queue_ != 0) {
    std::size_t by_queue = heap_.size() >= max_queue_ ? 0 : max_queue_ - heap_.size();
    return std::min(by_threads, by_queue);
  }
  return by_threads;
}

}  // namespace taskforge
