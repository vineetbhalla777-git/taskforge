#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <numeric>
#include <thread>
#include <vector>

#include "taskforge/thread_pool.hpp"

using namespace taskforge;
using namespace std::chrono_literals;

namespace {

// A reusable gate: tasks block on wait() until open() is called. Used to keep
// worker threads busy so we can build up a queue deterministically.
class Gate {
 public:
  void open() {
    { std::lock_guard<std::mutex> lk(m_); open_ = true; }
    cv_.notify_all();
  }
  void wait() {
    std::unique_lock<std::mutex> lk(m_);
    cv_.wait(lk, [&] { return open_; });
  }
 private:
  std::mutex m_;
  std::condition_variable cv_;
  bool open_ = false;
};

// Wait (bounded) until pred() is true; avoids sleeps-as-synchronisation.
template <class Pred>
bool wait_until(Pred pred, std::chrono::milliseconds limit = 5s) {
  auto end = std::chrono::steady_clock::now() + limit;
  while (std::chrono::steady_clock::now() < end) {
    if (pred()) return true;
    std::this_thread::sleep_for(1ms);
  }
  return pred();
}

}  // namespace

TEST(ThreadPool, RunsAllTasks) {
  ThreadPool pool({4, 0});
  std::atomic<int> n{0};
  for (int i = 0; i < 1000; ++i) ASSERT_TRUE(pool.try_post(0, [&] { ++n; }));
  pool.wait_idle();
  EXPECT_EQ(n.load(), 1000);
  auto s = pool.stats();
  EXPECT_EQ(s.submitted, 1000u);
  EXPECT_EQ(s.completed, 1000u);
  EXPECT_EQ(s.failed, 0u);
  EXPECT_EQ(s.queued, 0u);
  EXPECT_EQ(s.active, 0u);
}

TEST(ThreadPool, SubmitReturnsValueAndVoid) {
  ThreadPool pool({2, 0});
  auto f1 = pool.submit(0, [] { return 6 * 7; });
  auto f2 = pool.submit(0, [] {});
  EXPECT_EQ(f1.get(), 42);
  EXPECT_NO_THROW(f2.get());
}

TEST(ThreadPool, ExceptionsGoToFutureAndAreCounted) {
  ThreadPool pool({2, 0});
  auto f = pool.submit(0, []() -> int { throw std::runtime_error("boom"); });
  EXPECT_THROW(f.get(), std::runtime_error);
  pool.try_post(0, [] { throw 1; });  // fire-and-forget throwing task
  pool.wait_idle();
  auto s = pool.stats();
  EXPECT_EQ(s.failed, 2u);
  EXPECT_EQ(s.completed, 0u);
  // Pool must still be usable after failures.
  EXPECT_EQ(pool.submit(0, [] { return 1; }).get(), 1);
}

TEST(ThreadPool, HigherPriorityRunsFirst) {
  ThreadPool pool({1, 0});
  Gate gate;
  std::atomic<bool> blocker_started{false};
  pool.try_post(100, [&] { blocker_started = true; gate.wait(); });
  ASSERT_TRUE(wait_until([&] { return blocker_started.load(); }));

  std::mutex m;
  std::vector<int> order;
  for (int p : {1, 5, 3, 9, 0, 7}) {
    pool.try_post(p, [&, p] { std::lock_guard<std::mutex> lk(m); order.push_back(p); });
  }
  gate.open();
  pool.wait_idle();
  EXPECT_EQ(order, (std::vector<int>{9, 7, 5, 3, 1, 0}));
}

TEST(ThreadPool, FifoWithinSamePriority) {
  ThreadPool pool({1, 0});
  Gate gate;
  std::atomic<bool> started{false};
  pool.try_post(0, [&] { started = true; gate.wait(); });
  ASSERT_TRUE(wait_until([&] { return started.load(); }));

  std::mutex m;
  std::vector<int> order;
  for (int i = 0; i < 50; ++i) {
    pool.try_post(5, [&, i] { std::lock_guard<std::mutex> lk(m); order.push_back(i); });
  }
  gate.open();
  pool.wait_idle();
  std::vector<int> expected(50);
  std::iota(expected.begin(), expected.end(), 0);
  EXPECT_EQ(order, expected);
}

TEST(ThreadPool, ConcurrencyIsBoundedByThreadCount) {
  constexpr int kThreads = 4;
  ThreadPool pool({kThreads, 0});
  std::atomic<int> running{0}, peak{0};
  for (int i = 0; i < 40; ++i) {
    pool.try_post(0, [&] {
      int now = ++running;
      int p = peak.load();
      while (now > p && !peak.compare_exchange_weak(p, now)) {}
      std::this_thread::sleep_for(5ms);
      --running;
    });
  }
  pool.wait_idle();
  EXPECT_LE(peak.load(), kThreads);
  EXPECT_GE(peak.load(), 2);  // genuinely concurrent
}

TEST(ThreadPool, BoundedQueueRejectsWhenFull) {
  ThreadPool pool({1, 2});
  Gate gate;
  std::atomic<bool> started{false};
  ASSERT_TRUE(pool.try_post(0, [&] { started = true; gate.wait(); }));
  ASSERT_TRUE(wait_until([&] { return started.load(); }));  // running, not queued

  EXPECT_TRUE(pool.try_post(0, [] {}));
  EXPECT_TRUE(pool.try_post(0, [] {}));
  EXPECT_FALSE(pool.try_post(0, [] {}));  // queue holds 2 => third is rejected
  EXPECT_THROW(pool.submit(0, [] { return 1; }), QueueFullError);
  EXPECT_EQ(pool.stats().rejected, 2u);
  EXPECT_EQ(pool.free_slots(), 0u);
  gate.open();
  pool.wait_idle();
  EXPECT_EQ(pool.free_slots(), 1u);
}

TEST(ThreadPool, FreeSlotsTracksLoad) {
  ThreadPool pool({3, 0});
  EXPECT_EQ(pool.free_slots(), 3u);
  Gate gate;
  std::atomic<int> started{0};
  for (int i = 0; i < 2; ++i) pool.try_post(0, [&] { ++started; gate.wait(); });
  ASSERT_TRUE(wait_until([&] { return started.load() == 2; }));
  EXPECT_EQ(pool.free_slots(), 1u);
  gate.open();
  pool.wait_idle();
  EXPECT_EQ(pool.free_slots(), 3u);
}

TEST(ThreadPool, ShutdownDrainRunsQueuedWork) {
  std::atomic<int> n{0};
  {
    ThreadPool pool({2, 0});
    for (int i = 0; i < 200; ++i) pool.try_post(0, [&] { std::this_thread::sleep_for(100us); ++n; });
    pool.shutdown(true);
  }
  EXPECT_EQ(n.load(), 200);
}

TEST(ThreadPool, ShutdownWithoutDrainDiscardsQueuedWork) {
  ThreadPool pool({1, 0});
  Gate gate;
  std::atomic<bool> started{false};
  pool.try_post(0, [&] { started = true; gate.wait(); });
  ASSERT_TRUE(wait_until([&] { return started.load(); }));
  auto queued = pool.submit(0, [] { return 1; });

  std::thread stopper([&] { pool.shutdown(false); });
  // The queued task is discarded => its promise is destroyed.
  EXPECT_THROW(queued.get(), std::future_error);
  gate.open();  // let the running task finish so shutdown can join
  stopper.join();
}

TEST(ThreadPool, RejectsWorkAfterShutdown) {
  ThreadPool pool({2, 0});
  pool.shutdown();
  EXPECT_FALSE(pool.try_post(0, [] {}));
  EXPECT_THROW(pool.submit(0, [] { return 1; }), PoolStoppedError);
  pool.shutdown();  // idempotent
}

TEST(ThreadPool, WaitIdleReturnsImmediatelyWhenEmpty) {
  ThreadPool pool({2, 0});
  pool.wait_idle();
  SUCCEED();
}

TEST(ThreadPool, ManyProducersStress) {
  ThreadPool pool({4, 0});
  constexpr int kProducers = 8, kPer = 5000;
  std::atomic<long> sum{0};
  std::vector<std::thread> producers;
  for (int p = 0; p < kProducers; ++p) {
    producers.emplace_back([&, p] {
      for (int i = 0; i < kPer; ++i) pool.try_post(i % 10, [&, i] { sum += i; });
    });
  }
  for (auto& t : producers) t.join();
  pool.wait_idle();
  long expected = static_cast<long>(kProducers) * (static_cast<long>(kPer) * (kPer - 1) / 2);
  EXPECT_EQ(sum.load(), expected);
  EXPECT_EQ(pool.stats().completed, static_cast<std::uint64_t>(kProducers) * kPer);
}
