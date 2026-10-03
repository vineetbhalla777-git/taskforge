#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <vector>

#include "stb_image.h"
#include "stb_image_write.h"
#include "taskforge/db.hpp"
#include "taskforge/handlers.hpp"

using namespace taskforge;
using nlohmann::json;
namespace fs = std::filesystem;

namespace {
class HandlersTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char tmpl[] = "/tmp/tf_handlers_XXXXXX";
    ASSERT_NE(mkdtemp(tmpl), nullptr);
    dir = tmpl;
    fs::create_directories(dir + "/images");
    ctx.job_id = "11111111-2222-3333-4444-555555555555";
    ctx.attempt = 1;
    ctx.data_dir = dir;
    const char* d = std::getenv("TASKFORGE_TEST_DSN");
    ctx.dsn = d ? d : "";
    ctx.log = [this](const std::string& l, const std::string& m) { logs.push_back(l + ": " + m); };
  }
  void TearDown() override { fs::remove_all(dir); }

  // Gradient test image so resampling is visibly non-trivial.
  void make_image(const std::string& name, int w, int h) {
    std::vector<unsigned char> px(static_cast<size_t>(w) * h * 4);
    for (int y = 0; y < h; ++y)
      for (int x = 0; x < w; ++x) {
        auto* p = &px[(static_cast<size_t>(y) * w + x) * 4];
        p[0] = static_cast<unsigned char>(255 * x / w); p[1] = static_cast<unsigned char>(255 * y / h); p[2] = 128; p[3] = 255;
      }
    ASSERT_TRUE(stbi_write_png((dir + "/images/" + name).c_str(), w, h, 4, px.data(), w * 4));
  }
  static bool dims(const std::string& path, int& w, int& h) { int c; return stbi_info(path.c_str(), &w, &h, &c) != 0; }

  std::string dir;
  JobContext ctx;
  std::vector<std::string> logs;
};
}  // namespace

// ------------------------------------------------------------------ image_resize
TEST_F(HandlersTest, ResizeKeepsAspectRatio) {
  make_image("a.png", 200, 100);
  auto r = handle_image_resize(ctx, {{"source", "a.png"}, {"width", 50}, {"height", 50}});
  EXPECT_EQ(r["width"], 50);
  EXPECT_EQ(r["height"], 25);  // 200x100 fitted inside 50x50
  EXPECT_EQ(r["input_width"], 200);
  int w, h;
  ASSERT_TRUE(dims(dir + "/output/" + r["output"].get<std::string>(), w, h));
  EXPECT_EQ(w, 50); EXPECT_EQ(h, 25);
  EXPECT_FALSE(logs.empty());
}

TEST_F(HandlersTest, ResizeStretchAndJpeg) {
  make_image("a.png", 200, 100);
  auto r = handle_image_resize(ctx, {{"source", "a.png"}, {"width", 60}, {"height", 40}, {"keep_aspect", false}, {"format", "jpg"}});
  EXPECT_EQ(r["width"], 60); EXPECT_EQ(r["height"], 40);
  EXPECT_EQ(r["output"], ctx.job_id + ".jpg");
  int w, h;
  ASSERT_TRUE(dims(dir + "/output/" + r["output"].get<std::string>(), w, h));
  EXPECT_EQ(w, 60); EXPECT_EQ(h, 40);
}

TEST_F(HandlersTest, ResizeIsIdempotent) {
  make_image("a.png", 64, 64);
  json p = {{"source", "a.png"}, {"width", 16}, {"height", 16}};
  auto r1 = handle_image_resize(ctx, p);
  auto r2 = handle_image_resize(ctx, p);  // simulated at-least-once re-execution
  EXPECT_EQ(r1["output"], r2["output"]);
  int n = 0;
  for (auto& e : fs::directory_iterator(dir + "/output")) { (void)e; ++n; }
  EXPECT_EQ(n, 1);  // one result file, no temp leftovers
}

TEST_F(HandlersTest, ResizeRejectsBadInput) {
  make_image("a.png", 10, 10);
  std::ofstream(dir + "/images/garbage.png") << "this is not a png";
  std::vector<json> bad = {
      {{"source", "../secret.png"}, {"width", 5}, {"height", 5}},
      {{"source", "/etc/passwd"}, {"width", 5}, {"height", 5}},
      {{"source", "sub/a.png"}, {"width", 5}, {"height", 5}},
      {{"source", "a.png"}, {"width", 5000}, {"height", 5}},
      {{"source", "a.png"}, {"width", 0}, {"height", 5}},
      {{"source", "a.png"}, {"width", "5"}, {"height", 5}},
      {{"source", "a.png"}, {"width", 5}, {"height", 5}, {"format", "exe"}},
      {{"source", "a.png"}, {"width", 5}, {"height", 5}, {"extra", 1}},
      {{"source", "missing.png"}, {"width", 5}, {"height", 5}},
      {{"source", "garbage.png"}, {"width", 5}, {"height", 5}},
      {{"width", 5}, {"height", 5}},
      json::array(),
  };
  for (auto& p : bad) EXPECT_THROW(handle_image_resize(ctx, p), PermanentError) << p.dump();
}

// ------------------------------------------------------------------ flaky_api
TEST_F(HandlersTest, FlakyFailsFirstNThenSucceeds) {
  json p = {{"endpoint", "pay/charge"}, {"failure_rate", 0.0}, {"latency_ms", 0}, {"fail_first_n_attempts", 2}};
  ctx.attempt = 1; EXPECT_THROW(handle_flaky_api(ctx, p), TemporaryError);
  ctx.attempt = 2; EXPECT_THROW(handle_flaky_api(ctx, p), TemporaryError);
  ctx.attempt = 3;
  auto r = handle_flaky_api(ctx, p);
  EXPECT_EQ(r["http_status"], 200); EXPECT_EQ(r["attempt"], 3);
}

TEST_F(HandlersTest, FlakyRatesAndPermanentMode) {
  EXPECT_THROW(handle_flaky_api(ctx, {{"endpoint", "x"}, {"failure_rate", 1.0}, {"latency_ms", 0}}), TemporaryError);
  EXPECT_THROW(handle_flaky_api(ctx, {{"endpoint", "x"}, {"failure_rate", 1.0}, {"latency_ms", 0}, {"permanent", true}}), PermanentError);
  for (int i = 0; i < 20; ++i) EXPECT_NO_THROW(handle_flaky_api(ctx, {{"endpoint", "x"}, {"failure_rate", 0.0}, {"latency_ms", 0}}));
  // failure_rate is honoured statistically: ~30% of 2000 calls
  int fails = 0;
  for (int i = 0; i < 2000; ++i) {
    try { handle_flaky_api(ctx, {{"endpoint", "x"}, {"failure_rate", 0.3}, {"latency_ms", 0}}); } catch (const TemporaryError&) { ++fails; }
  }
  EXPECT_GT(fails, 480); EXPECT_LT(fails, 720);
}

TEST_F(HandlersTest, FlakyHonoursLatencyAndValidates) {
  auto t0 = std::chrono::steady_clock::now();
  handle_flaky_api(ctx, {{"endpoint", "x"}, {"failure_rate", 0.0}, {"latency_ms", 60}});
  EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count(), 55);
  for (json p : std::vector<json>{{{"endpoint", "x; rm -rf /"}}, {{"endpoint", "x"}, {"latency_ms", 999999}},
                                  {{"endpoint", "x"}, {"failure_rate", 2}}, {{"endpoint", "x"}, {"bogus", 1}}, {{"latency_ms", 1}}})
    EXPECT_THROW(handle_flaky_api(ctx, p), PermanentError) << p.dump();
}

// ------------------------------------------------------------------ generate_report
TEST_F(HandlersTest, ReportCsvAndJson) {
  if (ctx.dsn.empty()) GTEST_SKIP() << "TASKFORGE_TEST_DSN not set";
  db::Connection c(ctx.dsn);
  c.exec("TRUNCATE jobs, job_attempts, job_logs, dead_letters, webhook_deliveries, workers CASCADE");
  c.exec("INSERT INTO jobs (task_type, status) SELECT 'flaky_api', 'queued' FROM generate_series(1,3)");
  c.exec("INSERT INTO jobs (task_type, status, started_at, finished_at) VALUES "
         "('image_resize','succeeded', now() - interval '2 seconds', now()), "
         "('image_resize','succeeded', now() - interval '4 seconds', now())");
  c.exec("INSERT INTO jobs (task_type, status, created_at) VALUES ('flaky_api','queued', now() - interval '400 days')");  // outside window

  auto r = handle_generate_report(ctx, {{"days", 7}});
  EXPECT_EQ(r["rows"], 2);
  std::ifstream f(dir + "/output/" + r["output"].get<std::string>());
  std::stringstream ss; ss << f.rdbuf();
  EXPECT_EQ(ss.str(), "task_type,status,jobs,avg_runtime_ms\nflaky_api,queued,3,0\nimage_resize,succeeded,2,3000\n");

  ctx.job_id = "99999999-2222-3333-4444-555555555555";
  auto rj = handle_generate_report(ctx, {{"days", 365 }, {"format", "json"}});
  std::ifstream fj(dir + "/output/" + rj["output"].get<std::string>());
  json doc = json::parse(fj);
  EXPECT_EQ(doc["rows"].size(), 2u);
  EXPECT_EQ(doc["rows"][0]["task_type"], "flaky_api");

  EXPECT_THROW(handle_generate_report(ctx, {{"days", 0}}), PermanentError);
  EXPECT_THROW(handle_generate_report(ctx, {{"days", 7}, {"format", "xls"}}), PermanentError);
  EXPECT_THROW(handle_generate_report(ctx, {{"report_type", "drop_tables"}}), PermanentError);
}
