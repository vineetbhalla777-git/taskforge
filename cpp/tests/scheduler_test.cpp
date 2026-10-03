// End-to-end scheduler tests against real PostgreSQL (TASKFORGE_TEST_DSN), with fake handlers.
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <mutex>
#include <thread>
#include <vector>

#include "taskforge/db.hpp"
#include "taskforge/scheduler.hpp"

using namespace taskforge;
using namespace std::chrono_literals;
using nlohmann::json;

namespace {
std::string dsn() { const char* s = std::getenv("TASKFORGE_TEST_DSN"); return s ? s : ""; }

class SchedulerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (dsn().empty()) GTEST_SKIP() << "TASKFORGE_TEST_DSN not set";
    conn = std::make_unique<db::Connection>(dsn());
    conn->exec("TRUNCATE jobs, job_attempts, job_logs, dead_letters, webhook_deliveries, workers CASCADE");
  }
  SchedulerOptions opts(size_t threads) {
    SchedulerOptions o;
    o.dsn = dsn(); o.worker_id = "test-worker"; o.threads = threads; o.poll_ms = 20; o.lease_seconds = 30;
    return o;
  }
  std::string add(const std::string& label, int priority, const std::string& type = "test",
                  const std::string& run_at_sql = "now()") {
    return conn->exec("INSERT INTO jobs (task_type, priority, run_at, payload) VALUES ($1, $2::int, " + run_at_sql +
                          ", jsonb_build_object('label', $3::text)) RETURNING id::text",
                      {type, std::to_string(priority), label}).str(0, 0);
  }
  std::string scalar(const std::string& sql, const db::Params& p = {}) { return conn->exec(sql, p).str(0, 0); }
  int count(const std::string& status) { return std::stoi(scalar("SELECT count(*) FROM jobs WHERE status=$1", {status})); }
  bool wait_for(std::function<bool()> pred, std::chrono::milliseconds limit = 10s) {
    auto end = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < end) { if (pred()) return true; std::this_thread::sleep_for(5ms); }
    return pred();
  }
  bool wait_all_done(std::chrono::milliseconds limit = 10s) {
    return wait_for([&] { return std::stoi(scalar("SELECT count(*) FROM jobs WHERE status IN ('queued','running','retrying')")) == 0; }, limit);
  }
  std::unique_ptr<db::Connection> conn;
};

struct Recorder {
  std::mutex m;
  std::vector<std::string> order;
  void add(const std::string& s) { std::lock_guard<std::mutex> lk(m); order.push_back(s); }
};
}  // namespace

TEST_F(SchedulerTest, SuccessStoresResultLogsAndHistory) {
  auto id = add("x", 5);
  Scheduler s(opts(2));
  s.register_handler("test", [](JobContext& c, const json& p) {
    c.info("hello from handler");
    return json{{"echo", p["label"]}, {"attempt", c.attempt}};
  });
  s.start();
  ASSERT_TRUE(wait_all_done());
  s.stop();
  EXPECT_EQ(scalar("SELECT status FROM jobs WHERE id=$1::uuid", {id}), "succeeded");
  EXPECT_EQ(scalar("SELECT result->>'echo' FROM jobs WHERE id=$1::uuid", {id}), "x");
  EXPECT_EQ(scalar("SELECT worker_id FROM jobs WHERE id=$1::uuid", {id}), "test-worker");
  EXPECT_EQ(scalar("SELECT outcome FROM job_attempts WHERE job_id=$1::uuid", {id}), "succeeded");
  EXPECT_EQ(scalar("SELECT message FROM job_logs WHERE job_id=$1::uuid", {id}), "hello from handler");
  EXPECT_EQ(s.counters().succeeded, 1u);
}

TEST_F(SchedulerTest, PriorityOrderWithSingleThread) {
  for (int p : {1, 5, 3, 9, 0, 7}) add("p" + std::to_string(p), p);
  Recorder rec;
  Scheduler s(opts(1));
  s.register_handler("test", [&](JobContext&, const json& p) { rec.add(p["label"]); return json::object(); });
  s.start();
  ASSERT_TRUE(wait_all_done());
  s.stop();
  EXPECT_EQ(rec.order, (std::vector<std::string>{"p9", "p7", "p5", "p3", "p1", "p0"}));
}

TEST_F(SchedulerTest, HighPriorityJobJumpsAheadOfWaitingBacklog) {
  std::atomic<bool> release{false}, started{false};
  Recorder rec;
  Scheduler s(opts(1));
  s.register_handler("test", [&](JobContext&, const json& p) {
    std::string l = p["label"];
    if (l == "blocker") { started = true; while (!release) std::this_thread::sleep_for(2ms); }
    rec.add(l);
    return json::object();
  });
  add("blocker", 9);
  s.start();
  ASSERT_TRUE(wait_for([&] { return started.load(); }));
  for (int i = 0; i < 5; ++i) add("low" + std::to_string(i), 1);   // older, low priority backlog
  add("URGENT", 8);                                                 // newer but more important
  release = true;
  ASSERT_TRUE(wait_all_done());
  s.stop();
  ASSERT_EQ(rec.order.size(), 7u);
  EXPECT_EQ(rec.order[0], "blocker");
  EXPECT_EQ(rec.order[1], "URGENT");
}

TEST_F(SchedulerTest, DelayedJobRunsOnlyAfterItsRunAt) {
  auto id = add("later", 5, "test", "now() + interval '1500 milliseconds'");
  std::atomic<int> ran{0};
  Scheduler s(opts(2));
  s.register_handler("test", [&](JobContext&, const json&) { ++ran; return json::object(); });
  s.start();
  std::this_thread::sleep_for(700ms);
  EXPECT_EQ(ran.load(), 0);                         // not yet due
  EXPECT_EQ(scalar("SELECT status FROM jobs WHERE id=$1::uuid", {id}), "queued");
  ASSERT_TRUE(wait_all_done(6s));
  s.stop();
  EXPECT_EQ(ran.load(), 1);
  EXPECT_EQ(scalar("SELECT (started_at >= run_at)::text FROM jobs WHERE id=$1::uuid", {id}), "true");
}

TEST_F(SchedulerTest, RunsJobsConcurrentlyUpToThreadLimit) {
  for (int i = 0; i < 8; ++i) add("j" + std::to_string(i), 5);
  std::atomic<int> running{0}, peak{0};
  Scheduler s(opts(4));
  s.register_handler("test", [&](JobContext&, const json&) {
    int now = ++running;
    int p = peak.load();
    while (now > p && !peak.compare_exchange_weak(p, now)) {}
    std::this_thread::sleep_for(200ms);
    --running;
    return json::object();
  });
  auto t0 = std::chrono::steady_clock::now();
  s.start();
  ASSERT_TRUE(wait_all_done());
  auto elapsed = std::chrono::steady_clock::now() - t0;
  s.stop();
  EXPECT_EQ(peak.load(), 4);
  EXPECT_EQ(count("succeeded"), 8);
  // 8 jobs x 200ms serial = 1.6s; with 4 workers ~0.4s. Generous bound to stay robust on slow CI.
  EXPECT_LT(elapsed, 1200ms);
}

TEST_F(SchedulerTest, NeverClaimsMoreJobsThanItCanRun) {
  for (int i = 0; i < 12; ++i) add("j" + std::to_string(i), 5);
  Scheduler s(opts(2));
  s.register_handler("test", [](JobContext&, const json&) { std::this_thread::sleep_for(60ms); return json::object(); });
  std::atomic<bool> done{false};
  int max_running = 0;
  std::thread sampler([&] {
    db::Connection c(dsn());
    while (!done) { max_running = std::max(max_running, c.exec("SELECT count(*)::int FROM jobs WHERE status='running'").integer(0, 0)); std::this_thread::sleep_for(3ms); }
  });
  s.start();
  ASSERT_TRUE(wait_all_done());
  done = true; sampler.join();
  s.stop();
  EXPECT_LE(max_running, 2);
  EXPECT_GE(max_running, 1);
}

TEST_F(SchedulerTest, TemporaryErrorRetriesThenDeadLettersOnExhaustion) {
  // max_retries defaults to 3 in the schema; force it to 1 so this finishes fast.
  auto id = add("bad", 5);
  conn->exec("UPDATE jobs SET max_retries = 1 WHERE id = $1::uuid", {id});
  Scheduler s(opts(1));
  s.register_handler("test", [](JobContext&, const json&) -> json { throw TemporaryError("HTTP 503 from upstream"); });
  s.start();
  // First failure (attempt 1 of max_retries=1): retried, not yet dead.
  ASSERT_TRUE(wait_for([&] { return scalar("SELECT status FROM jobs WHERE id=$1::uuid", {id}) == "retrying"; }));
  EXPECT_EQ(scalar("SELECT worker_id FROM jobs WHERE id=$1::uuid", {id}), "");  // cleared while waiting
  EXPECT_GT(std::stod(scalar("SELECT EXTRACT(EPOCH FROM (run_at - now())) FROM jobs WHERE id=$1::uuid", {id})), -0.01);
  ASSERT_TRUE(wait_all_done(8s));  // backoff elapses, it's reclaimed, fails again, now exhausted
  s.stop();
  EXPECT_EQ(scalar("SELECT status FROM jobs WHERE id=$1::uuid", {id}), "dead");
  EXPECT_EQ(scalar("SELECT last_error FROM jobs WHERE id=$1::uuid", {id}), "HTTP 503 from upstream");
  EXPECT_EQ(scalar("SELECT reason FROM dead_letters WHERE job_id=$1::uuid", {id}), "retries_exhausted");
  EXPECT_EQ(scalar("SELECT attempts FROM jobs WHERE id=$1::uuid", {id}), "2");  // 1 initial + 1 retry
  EXPECT_EQ(scalar("SELECT count(*) FROM job_attempts WHERE job_id=$1::uuid AND outcome='failed'", {id}), "2");
  EXPECT_GE(s.counters().retried, 1u);
  EXPECT_GE(s.counters().dead, 1u);
}

TEST_F(SchedulerTest, PermanentErrorSkipsRetryEvenWithRetriesRemaining) {
  auto id = add("bad", 5);
  conn->exec("UPDATE jobs SET max_retries = 5 WHERE id = $1::uuid", {id});  // plenty of retries available
  Scheduler s(opts(1));
  s.register_handler("test", [](JobContext&, const json&) -> json { throw PermanentError("HTTP 400 bad request"); });
  s.start();
  ASSERT_TRUE(wait_all_done());
  s.stop();
  EXPECT_EQ(scalar("SELECT status FROM jobs WHERE id=$1::uuid", {id}), "dead");
  EXPECT_EQ(scalar("SELECT attempts FROM jobs WHERE id=$1::uuid", {id}), "1");  // never retried
  EXPECT_EQ(scalar("SELECT reason FROM dead_letters WHERE job_id=$1::uuid", {id}), "permanent_error");
  EXPECT_EQ(s.counters().dead, 1u);
  EXPECT_EQ(s.counters().retried, 0u);
}

TEST_F(SchedulerTest, RetryBackoffGrowsExponentially) {
  auto id = add("bad", 5);
  conn->exec("UPDATE jobs SET max_retries = 5 WHERE id = $1::uuid", {id});
  JobStore store(*conn);
  double prev_delay = -1;
  for (int attempt = 1; attempt <= 3; ++attempt) {
    auto jobs = store.claim("w1", 1, 30);
    ASSERT_EQ(jobs.size(), 1u);
    ASSERT_EQ(jobs[0].attempt, attempt);
    auto outcome = store.retry_or_dead(id, "w1", attempt, "boom", /*base=*/1.0, /*max=*/1000.0);
    ASSERT_EQ(outcome, JobStore::RetryOutcome::Retried);
    double delay = std::stod(scalar("SELECT EXTRACT(EPOCH FROM (run_at - now())) FROM jobs WHERE id=$1::uuid", {id}));
    EXPECT_GT(delay, prev_delay);  // 2^(a-1) grows even with jitter halving the ceiling
    prev_delay = delay;
    conn->exec("UPDATE jobs SET run_at = now() WHERE id = $1::uuid", {id});  // don't actually wait out the backoff
  }
}

TEST_F(SchedulerTest, UnknownTaskTypeIsDeadLettered) {
  auto id = add("x", 5, "no_such_handler");
  Scheduler s(opts(1));
  s.start();
  ASSERT_TRUE(wait_all_done());
  s.stop();
  EXPECT_EQ(scalar("SELECT status FROM jobs WHERE id=$1::uuid", {id}), "dead");
  EXPECT_NE(scalar("SELECT last_error FROM jobs WHERE id=$1::uuid", {id}).find("no handler registered"), std::string::npos);
  EXPECT_EQ(scalar("SELECT reason FROM dead_letters WHERE job_id=$1::uuid", {id}), "permanent_error");
}

TEST_F(SchedulerTest, GracefulStopFinishesRunningJobAndClaimsNoMore) {
  auto first = add("first", 9);
  for (int i = 0; i < 3; ++i) add("rest" + std::to_string(i), 1);
  std::atomic<bool> started{false};
  Scheduler s(opts(1));
  s.register_handler("test", [&](JobContext&, const json&) { started = true; std::this_thread::sleep_for(300ms); return json::object(); });
  s.start();
  ASSERT_TRUE(wait_for([&] { return started.load(); }));
  s.stop();  // must wait for the running job
  EXPECT_EQ(scalar("SELECT status FROM jobs WHERE id=$1::uuid", {first}), "succeeded");
  EXPECT_EQ(count("running"), 0);
  EXPECT_GE(count("queued"), 2);  // the rest were left in the queue for other workers
}

TEST_F(SchedulerTest, TwoSchedulersShareTheQueueWithoutDuplicates) {
  constexpr int kJobs = 60;
  for (int i = 0; i < kJobs; ++i) add("j" + std::to_string(i), i % 10);
  std::atomic<int> a_runs{0}, b_runs{0};
  auto o1 = opts(3); o1.worker_id = "A";
  auto o2 = opts(3); o2.worker_id = "B";
  Scheduler a(o1), b(o2);
  a.register_handler("test", [&](JobContext&, const json&) { ++a_runs; std::this_thread::sleep_for(20ms); return json::object(); });
  b.register_handler("test", [&](JobContext&, const json&) { ++b_runs; std::this_thread::sleep_for(20ms); return json::object(); });
  a.start(); b.start();
  ASSERT_TRUE(wait_all_done());
  a.stop(); b.stop();
  EXPECT_EQ(a_runs + b_runs, kJobs);  // each job executed exactly once
  EXPECT_EQ(count("succeeded"), kJobs);
  EXPECT_EQ(scalar("SELECT count(*) FROM job_attempts"), std::to_string(kJobs));
  EXPECT_GT(a_runs.load(), 0); EXPECT_GT(b_runs.load(), 0);
}
