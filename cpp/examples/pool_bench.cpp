// Measures real thread-pool numbers on the machine it runs on.
//   1. no-op task throughput (queue/lock overhead)
//   2. 10 ms sleeping tasks (I/O-like) - shows concurrency scaling with threads
//   3. priority latency: a high-priority task submitted behind a backlog
#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include "taskforge/thread_pool.hpp"
using namespace taskforge;
using Clock = std::chrono::steady_clock;
static double secs(Clock::time_point a, Clock::time_point b) { return std::chrono::duration<double>(b - a).count(); }

int main() {
  std::printf("hardware_concurrency=%u\n", std::thread::hardware_concurrency());
  for (size_t threads : {1u, 2u, 4u, 8u}) {
    ThreadPool pool({threads, 0});
    const int N = 200000;
    std::atomic<int> n{0};
    auto t0 = Clock::now();
    for (int i = 0; i < N; ++i) pool.try_post(i % 10, [&] { ++n; });
    pool.wait_idle();
    double s = secs(t0, Clock::now());
    std::printf("noop        threads=%-3zu tasks=%d  %.0f tasks/s\n", threads, N, N / s);
  }
  for (size_t threads : {1u, 4u, 16u, 64u}) {
    ThreadPool pool({threads, 0});
    const int N = 400;
    auto t0 = Clock::now();
    for (int i = 0; i < N; ++i) pool.try_post(0, [] { std::this_thread::sleep_for(std::chrono::milliseconds(10)); });
    pool.wait_idle();
    double s = secs(t0, Clock::now());
    std::printf("10ms-sleep  threads=%-3zu tasks=%d  %.2fs  %.0f tasks/s (ideal %.0f)\n", threads, N, s, N / s, threads * 100.0);
  }
  {
    ThreadPool pool({4, 0});
    for (int i = 0; i < 2000; ++i) pool.try_post(0, [] { std::this_thread::sleep_for(std::chrono::milliseconds(2)); });
    auto t0 = Clock::now();
    auto hi = pool.submit(9, [t0] { return secs(t0, Clock::now()); });
    double hi_wait = hi.get();
    auto t1 = Clock::now();
    auto lo = pool.submit(-1, [t1] { return secs(t1, Clock::now()); });
    double lo_wait = lo.get();
    std::printf("priority: high-priority ran after %.1f ms behind a 2000-task backlog; low-priority ran after %.1f ms\n", hi_wait * 1e3, lo_wait * 1e3);
  }
}
