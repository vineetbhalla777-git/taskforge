// Measures how fast N concurrent claimers can drain a queue (claim only, no handler work).
//   claim_bench "<dsn>" [jobs=20000] [batch=50]
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>
#include "taskforge/db.hpp"
#include "taskforge/job_store.hpp"
using namespace taskforge;
using Clock = std::chrono::steady_clock;

int main(int argc, char** argv) {
  if (argc < 2) { std::fprintf(stderr, "usage: claim_bench <dsn> [jobs] [batch]\n"); return 2; }
  std::string dsn = argv[1];
  int jobs = argc > 2 ? std::atoi(argv[2]) : 20000;
  int batch = argc > 3 ? std::atoi(argv[3]) : 50;
  for (int claimers : {1, 2, 4, 8}) {
    db::Connection setup(dsn);
    setup.exec("TRUNCATE jobs, job_attempts CASCADE");
    setup.exec("INSERT INTO jobs (task_type, priority) SELECT 'bench', g % 10 FROM generate_series(1, $1::int) g", {std::to_string(jobs)});
    setup.exec("ANALYZE jobs");
    std::atomic<long> claimed{0};
    std::vector<double> lat_ms;
    std::vector<std::vector<double>> per(claimers);
    auto t0 = Clock::now();
    std::vector<std::thread> ts;
    for (int c = 0; c < claimers; ++c) ts.emplace_back([&, c] {
      db::Connection conn(dsn); JobStore s(conn);
      for (;;) {
        auto a = Clock::now();
        auto b = s.claim("bench-" + std::to_string(c), batch, 60);
        per[c].push_back(std::chrono::duration<double, std::milli>(Clock::now() - a).count());
        if (b.empty()) break;
        claimed += static_cast<long>(b.size());
      }
    });
    for (auto& t : ts) t.join();
    double secs = std::chrono::duration<double>(Clock::now() - t0).count();
    for (auto& v : per) lat_ms.insert(lat_ms.end(), v.begin(), v.end());
    std::sort(lat_ms.begin(), lat_ms.end());
    std::printf("claimers=%d batch=%d claimed=%ld in %.2fs => %.0f jobs/s | claim-call latency p50=%.2fms p99=%.2fms\n",
                claimers, batch, claimed.load(), secs, claimed / secs,
                lat_ms[lat_ms.size() / 2], lat_ms[static_cast<size_t>(lat_ms.size() * 0.99)]);
  }
}
