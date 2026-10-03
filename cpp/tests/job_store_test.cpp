// Integration tests: need a real PostgreSQL with db/schema.sql applied.
//   export TASKFORGE_TEST_DSN="host=localhost dbname=taskforge_test user=taskforge password=taskforge"
// Without the variable the tests are SKIPPED (not failed).
#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <memory>
#include <set>
#include <thread>
#include <vector>

#include "taskforge/db.hpp"
#include "taskforge/job_store.hpp"

using namespace taskforge;

namespace {
std::string dsn() { const char* s = std::getenv("TASKFORGE_TEST_DSN"); return s ? s : ""; }

class JobStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (dsn().empty()) GTEST_SKIP() << "TASKFORGE_TEST_DSN not set";
    conn = std::make_unique<db::Connection>(dsn());
    conn->exec("TRUNCATE jobs, job_attempts, job_logs, dead_letters, webhook_deliveries, workers CASCADE");
    store = std::make_unique<JobStore>(*conn);
  }
  std::string add(const std::string& type, int priority, int run_in_secs = 0,
                  const std::string& status = "queued") {
    return conn->exec(
                 "INSERT INTO jobs (task_type, priority, run_at, status, payload) "
                 "VALUES ($1, $2::int, now() + make_interval(secs => $3::int), $4, '{\"n\":1}') RETURNING id::text",
                 {type, std::to_string(priority), std::to_string(run_in_secs), status})
        .str(0, 0);
  }
  std::string scalar(const std::string& sql, const db::Params& p = {}) { return conn->exec(sql, p).str(0, 0); }
  std::unique_ptr<db::Connection> conn;
  std::unique_ptr<JobStore> store;
};
}  // namespace

TEST_F(JobStoreTest, ClaimsHighestPriorityFirstThenOldest) {
  auto low = add("t", 1);
  auto high_a = add("t", 9);
  auto mid = add("t", 5);
  auto high_b = add("t", 9);
  auto jobs = store->claim("w1", 10, 30);
  ASSERT_EQ(jobs.size(), 4u);
  EXPECT_EQ(jobs[0].id, high_a);  // same priority => created first wins
  EXPECT_EQ(jobs[1].id, high_b);
  EXPECT_EQ(jobs[2].id, mid);
  EXPECT_EQ(jobs[3].id, low);
}

TEST_F(JobStoreTest, RespectsLimit) {
  for (int i = 0; i < 10; ++i) add("t", i % 10);
  auto jobs = store->claim("w1", 3, 30);
  ASSERT_EQ(jobs.size(), 3u);
  EXPECT_EQ(jobs[0].priority, 9);
  EXPECT_EQ(jobs[1].priority, 8);
  EXPECT_EQ(jobs[2].priority, 7);
  EXPECT_TRUE(store->claim("w1", 0, 30).empty());
}

TEST_F(JobStoreTest, DelayedJobsAreNotClaimedUntilDue) {
  auto future = add("t", 9, /*run_in_secs=*/3600);
  auto now = add("t", 1);
  auto jobs = store->claim("w1", 10, 30);
  ASSERT_EQ(jobs.size(), 1u);
  EXPECT_EQ(jobs[0].id, now);
  conn->exec("UPDATE jobs SET run_at = now() - interval '1 second' WHERE id = $1::uuid", {future});
  jobs = store->claim("w1", 10, 30);
  ASSERT_EQ(jobs.size(), 1u);
  EXPECT_EQ(jobs[0].id, future);
}

TEST_F(JobStoreTest, OnlyQueuedAndRetryingAreClaimable) {
  add("t", 5, 0, "succeeded");
  add("t", 5, 0, "cancelled");
  add("t", 5, 0, "dead");
  auto retrying = add("t", 5, 0, "retrying");
  auto jobs = store->claim("w1", 10, 30);
  ASSERT_EQ(jobs.size(), 1u);
  EXPECT_EQ(jobs[0].id, retrying);
}

TEST_F(JobStoreTest, ClaimSetsRunningLeaseWorkerAttemptAndHistory) {
  auto id = add("resize", 5);
  auto jobs = store->claim("worker-A", 1, 60);
  ASSERT_EQ(jobs.size(), 1u);
  EXPECT_EQ(jobs[0].task_type, "resize");
  EXPECT_EQ(jobs[0].attempt, 1);
  EXPECT_EQ(scalar("SELECT status FROM jobs WHERE id=$1::uuid", {id}), "running");
  EXPECT_EQ(scalar("SELECT worker_id FROM jobs WHERE id=$1::uuid", {id}), "worker-A");
  // lease is ~60s in the future
  EXPECT_EQ(scalar("SELECT (lease_expires_at > now() + interval '55 seconds' AND "
                   "lease_expires_at < now() + interval '65 seconds')::text FROM jobs WHERE id=$1::uuid", {id}),
            "true");
  EXPECT_EQ(scalar("SELECT count(*)::text FROM job_attempts WHERE job_id=$1::uuid AND attempt=1 "
                   "AND outcome='running' AND worker_id='worker-A'", {id}), "1");
  // A running job is not claimable again.
  EXPECT_TRUE(store->claim("worker-B", 10, 60).empty());
}

TEST_F(JobStoreTest, ClaimIsDurableBeforeExecution) {
  add("t", 5);
  ASSERT_EQ(store->claim("w1", 1, 30).size(), 1u);
  db::Connection other(dsn());  // a brand-new session must already see RUNNING
  EXPECT_EQ(other.exec("SELECT status FROM jobs").str(0, 0), "running");
}

TEST_F(JobStoreTest, CompleteIsFencedByWorkerAndAttempt) {
  auto id = add("t", 5);
  auto jobs = store->claim("w1", 1, 30);
  ASSERT_EQ(jobs.size(), 1u);
  EXPECT_FALSE(store->complete(id, "intruder", 1, "{}"));
  EXPECT_FALSE(store->complete(id, "w1", 2, "{}"));  // wrong attempt
  EXPECT_EQ(scalar("SELECT status FROM jobs WHERE id=$1::uuid", {id}), "running");
  EXPECT_TRUE(store->complete(id, "w1", 1, "{\"ok\":true}"));
  EXPECT_EQ(scalar("SELECT status FROM jobs WHERE id=$1::uuid", {id}), "succeeded");
  EXPECT_EQ(scalar("SELECT result->>'ok' FROM jobs WHERE id=$1::uuid", {id}), "true");
  EXPECT_EQ(scalar("SELECT outcome FROM job_attempts WHERE job_id=$1::uuid", {id}), "succeeded");
  EXPECT_FALSE(store->complete(id, "w1", 1, "{}"));  // double-complete rejected
}

// The core guarantee: N independent sessions hammering the queue at once never
// receive the same job, and together they receive every job exactly once.
TEST_F(JobStoreTest, ConcurrentClaimersNeverGetTheSameJob) {
  constexpr int kJobs = 2000, kClaimers = 8;
  conn->exec("INSERT INTO jobs (task_type, priority) SELECT 't', g % 10 FROM generate_series(1, $1::int) g",
             {std::to_string(kJobs)});

  std::vector<std::vector<std::string>> got(kClaimers);
  std::atomic<int> errors{0};
  std::vector<std::thread> ts;
  for (int c = 0; c < kClaimers; ++c) {
    ts.emplace_back([&, c] {
      try {
        db::Connection my(dsn());  // one connection per thread
        JobStore s(my);
        for (;;) {
          auto batch = s.claim("w" + std::to_string(c), 10, 30);
          if (batch.empty()) break;
          for (auto& j : batch) got[c].push_back(j.id);
        }
      } catch (const std::exception& e) { ++errors; ADD_FAILURE() << e.what(); }
    });
  }
  for (auto& t : ts) t.join();
  ASSERT_EQ(errors.load(), 0);

  std::set<std::string> unique;
  size_t total = 0;
  int claimers_with_work = 0;
  for (auto& v : got) { total += v.size(); unique.insert(v.begin(), v.end()); claimers_with_work += !v.empty(); }
  EXPECT_EQ(total, static_cast<size_t>(kJobs));       // nothing lost
  EXPECT_EQ(unique.size(), static_cast<size_t>(kJobs));  // nothing duplicated
  EXPECT_EQ(scalar("SELECT count(*)::text FROM jobs WHERE status='running'"), std::to_string(kJobs));
  EXPECT_EQ(scalar("SELECT count(*)::text FROM job_attempts"), std::to_string(kJobs));
  RecordProperty("claimers_with_work", claimers_with_work);
}

// ---------------------------------------------------------------- Phase 5/6: retries, leases, heartbeats
TEST_F(JobStoreTest, RetryOrDeadReschedulesWithBackoffUntilExhausted) {
  // max_retries=1 => one retry allowed beyond the initial attempt (2 tries total).
  auto id = add("t", 5);
  conn->exec("UPDATE jobs SET max_retries = 1 WHERE id = $1::uuid", {id});

  auto jobs = store->claim("w1", 1, 30);
  ASSERT_EQ(jobs.size(), 1u);
  auto out1 = store->retry_or_dead(id, "w1", 1, "temp fail 1", 1.0, 1000.0);
  EXPECT_EQ(out1, JobStore::RetryOutcome::Retried);  // attempts(1) <= max_retries(1): budget remains
  EXPECT_EQ(scalar("SELECT status FROM jobs WHERE id=$1::uuid", {id}), "retrying");
  EXPECT_EQ(scalar("SELECT worker_id FROM jobs WHERE id=$1::uuid", {id}), "");
  EXPECT_TRUE(store->claim("w1", 10, 30).empty());  // not due yet (backoff hasn't elapsed)

  conn->exec("UPDATE jobs SET run_at = now() WHERE id = $1::uuid", {id});
  jobs = store->claim("w2", 1, 30);
  ASSERT_EQ(jobs.size(), 1u);
  EXPECT_EQ(jobs[0].attempt, 2);
  auto out2 = store->retry_or_dead(id, "w2", 2, "temp fail 2", 1.0, 1000.0);
  EXPECT_EQ(out2, JobStore::RetryOutcome::Dead);  // attempts(2) > max_retries(1): budget exhausted
  EXPECT_EQ(scalar("SELECT status FROM jobs WHERE id=$1::uuid", {id}), "dead");
  EXPECT_EQ(scalar("SELECT reason FROM dead_letters WHERE job_id=$1::uuid", {id}), "retries_exhausted");
  EXPECT_EQ(scalar("SELECT last_error FROM jobs WHERE id=$1::uuid", {id}), "temp fail 2");
}

TEST_F(JobStoreTest, RetryOrDeadIsFencedLikeComplete) {
  auto id = add("t", 5);
  store->claim("w1", 1, 30);
  EXPECT_EQ(store->retry_or_dead(id, "intruder", 1, "x"), JobStore::RetryOutcome::NotOwned);
  EXPECT_EQ(store->retry_or_dead(id, "w1", 99, "x"), JobStore::RetryOutcome::NotOwned);
  EXPECT_EQ(scalar("SELECT status FROM jobs WHERE id=$1::uuid", {id}), "running");
}

TEST_F(JobStoreTest, ReapExpiredLeasesRecoversCrashedJobs) {
  auto crashed = add("t", 5);
  auto healthy = add("t", 5);
  store->claim("dead-worker", 2, 60);
  // Simulate "dead-worker" crashing: force its lease into the past.
  conn->exec("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1::uuid", {crashed});
  int n = store->reap_expired_leases(1.0, 1000.0);
  EXPECT_EQ(n, 1);
  EXPECT_EQ(scalar("SELECT status FROM jobs WHERE id=$1::uuid", {crashed}), "retrying");
  EXPECT_EQ(scalar("SELECT status FROM jobs WHERE id=$1::uuid", {healthy}), "running");  // untouched: lease still valid
  EXPECT_EQ(scalar("SELECT outcome FROM job_attempts WHERE job_id=$1::uuid AND attempt=1", {crashed}), "lease_expired");
  EXPECT_NE(scalar("SELECT last_error FROM jobs WHERE id=$1::uuid", {crashed}).find("crashed"), std::string::npos);
  conn->exec("UPDATE jobs SET run_at = now() WHERE id = $1::uuid", {crashed});
  EXPECT_EQ(store->claim("recovery-worker", 10, 30).size(), 1u);  // claimable again, at-least-once delivery
}

TEST_F(JobStoreTest, ReapRespectsMaxRetriesTooCrashLoopEventuallyDies) {
  auto id = add("t", 5);
  conn->exec("UPDATE jobs SET max_retries = 0 WHERE id = $1::uuid", {id});  // no retries budget at all
  store->claim("dead-worker", 1, 60);
  conn->exec("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1::uuid", {id});
  EXPECT_EQ(store->reap_expired_leases(), 1);
  EXPECT_EQ(scalar("SELECT status FROM jobs WHERE id=$1::uuid", {id}), "dead");
  EXPECT_EQ(scalar("SELECT reason FROM dead_letters WHERE job_id=$1::uuid", {id}), "retries_exhausted");
}

TEST_F(JobStoreTest, HeartbeatUpsertsWorkerRow) {
  store->heartbeat("w1", "host-a", 4);
  EXPECT_EQ(scalar("SELECT hostname FROM workers WHERE worker_id='w1'"), "host-a");
  EXPECT_EQ(scalar("SELECT concurrency FROM workers WHERE worker_id='w1'"), "4");
  auto first_seen = scalar("SELECT last_heartbeat::text FROM workers WHERE worker_id='w1'");
  store->heartbeat("w1", "host-a", 8);  // concurrency changed, re-heartbeat
  EXPECT_EQ(scalar("SELECT concurrency FROM workers WHERE worker_id='w1'"), "8");
  EXPECT_EQ(scalar("SELECT count(*) FROM workers"), "1");
}
