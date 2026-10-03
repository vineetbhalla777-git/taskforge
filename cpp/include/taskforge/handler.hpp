// Task-handler interface. A handler receives the (already schema-validated by the
// API, and re-validated here) JSON payload and returns a JSON result.
//
// Throw TemporaryError for failures worth retrying (timeouts, HTTP 503, ...) and
// PermanentError for failures that will never succeed (bad input, HTTP 400).
// Any other exception is treated as temporary.
//
// Delivery is at-least-once, so handlers must be idempotent: the built-in ones
// write to a path derived from the job id and rename atomically, so re-running
// a job overwrites the same output instead of duplicating it.
#pragma once
#include <functional>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>

namespace taskforge {

struct TemporaryError : std::runtime_error { using std::runtime_error::runtime_error; };
struct PermanentError : std::runtime_error { using std::runtime_error::runtime_error; };

struct JobContext {
  std::string job_id;
  int attempt = 1;        // 1-based
  int max_retries = 0;
  std::string worker_id;
  std::string dsn;        // for handlers that need the database (reports)
  std::string data_dir;   // <data_dir>/images (inputs), <data_dir>/output (results)
  std::function<void(const std::string& level, const std::string& msg)> log;  // -> job_logs
  void info(const std::string& m) const { if (log) log("info", m); }
  void warn(const std::string& m) const { if (log) log("warn", m); }
};

using Handler = std::function<nlohmann::json(JobContext&, const nlohmann::json& payload)>;

}  // namespace taskforge
