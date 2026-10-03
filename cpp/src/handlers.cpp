#include "taskforge/handlers.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <random>
#include <regex>
#include <set>
#include <vector>
#include <thread>

#include "stb_image.h"
#include "stb_image_resize2.h"
#include "stb_image_write.h"
#include "taskforge/db.hpp"
#include "taskforge/scheduler.hpp"

namespace fs = std::filesystem;
using nlohmann::json;

namespace taskforge {
namespace {

// ---- payload validation helpers (defence in depth: the API already validated) ------
void reject_unknown(const json& p, const std::set<std::string>& allowed) {
  if (!p.is_object()) throw PermanentError("invalid payload: expected a JSON object");
  for (auto it = p.begin(); it != p.end(); ++it)
    if (!allowed.count(it.key())) throw PermanentError("invalid payload: unknown field '" + it.key() + "'");
}
long get_int(const json& p, const char* k, long lo, long hi, std::optional<long> def = std::nullopt) {
  auto it = p.find(k);
  if (it == p.end()) {
    if (def) return *def;
    throw PermanentError(std::string("invalid payload: missing '") + k + "'");
  }
  if (!it->is_number_integer()) throw PermanentError(std::string("invalid payload: '") + k + "' must be an integer");
  long v = it->get<long>();
  if (v < lo || v > hi) throw PermanentError(std::string("invalid payload: '") + k + "' out of range");
  return v;
}
std::string get_str(const json& p, const char* k, std::optional<std::string> def = std::nullopt) {
  auto it = p.find(k);
  if (it == p.end()) {
    if (def) return *def;
    throw PermanentError(std::string("invalid payload: missing '") + k + "'");
  }
  if (!it->is_string()) throw PermanentError(std::string("invalid payload: '") + k + "' must be a string");
  return it->get<std::string>();
}
bool get_bool(const json& p, const char* k, bool def) {
  auto it = p.find(k);
  if (it == p.end()) return def;
  if (!it->is_boolean()) throw PermanentError(std::string("invalid payload: '") + k + "' must be a boolean");
  return it->get<bool>();
}

// Write via temp file + rename so readers never see a partial result and a
// re-executed job overwrites the same path (idempotent).
fs::path output_path(const JobContext& ctx, const std::string& ext) {
  fs::path dir = fs::path(ctx.data_dir) / "output";
  fs::create_directories(dir);
  return dir / (ctx.job_id + "." + ext);
}
fs::path tmp_of(const fs::path& final_path) { return final_path.string() + ".tmp"; }

}  // namespace

// ============================== image_resize ==========================================
json handle_image_resize(JobContext& ctx, const json& p) {
  reject_unknown(p, {"source", "width", "height", "keep_aspect", "format"});
  const std::string source = get_str(p, "source");
  static const std::regex safe_name(R"(^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$)");
  if (!std::regex_match(source, safe_name) || source.find("..") != std::string::npos)
    throw PermanentError("invalid payload: 'source' must be a plain file name");
  const int W = static_cast<int>(get_int(p, "width", 1, kMaxImageDim));
  const int H = static_cast<int>(get_int(p, "height", 1, kMaxImageDim));
  const bool keep = get_bool(p, "keep_aspect", true);
  const std::string format = get_str(p, "format", std::string("png"));
  if (format != "png" && format != "jpg") throw PermanentError("invalid payload: 'format' must be png or jpg");

  const fs::path in_path = fs::path(ctx.data_dir) / "images" / source;
  int iw = 0, ih = 0, comp = 0;
  if (!stbi_info(in_path.c_str(), &iw, &ih, &comp))
    throw PermanentError("cannot read image '" + source + "' (missing or unsupported format)");
  if (static_cast<long>(iw) * ih > kMaxInputPixels)
    throw PermanentError("input image too large: " + std::to_string(iw) + "x" + std::to_string(ih));

  ctx.info("resizing " + source + " " + std::to_string(iw) + "x" + std::to_string(ih));
  std::unique_ptr<unsigned char, decltype(&stbi_image_free)> pixels(
      stbi_load(in_path.c_str(), &iw, &ih, &comp, 4), &stbi_image_free);
  if (!pixels) throw PermanentError("corrupt image '" + source + "'");

  int ow = W, oh = H;
  if (keep) {
    double scale = std::min(static_cast<double>(W) / iw, static_cast<double>(H) / ih);
    ow = std::max(1, static_cast<int>(std::lround(iw * scale)));
    oh = std::max(1, static_cast<int>(std::lround(ih * scale)));
  }
  std::vector<unsigned char> out(static_cast<size_t>(ow) * oh * 4);
  if (!stbir_resize_uint8_srgb(pixels.get(), iw, ih, 0, out.data(), ow, oh, 0, STBIR_RGBA))
    throw TemporaryError("resize failed (out of memory?)");

  const fs::path final_path = output_path(ctx, format);
  const fs::path tmp = tmp_of(final_path);
  int ok = (format == "png") ? stbi_write_png(tmp.c_str(), ow, oh, 4, out.data(), ow * 4)
                             : stbi_write_jpg(tmp.c_str(), ow, oh, 4, out.data(), 90);
  if (!ok) throw TemporaryError("could not write output file");  // disk problems are worth retrying
  fs::rename(tmp, final_path);

  ctx.info("wrote " + final_path.filename().string());
  return {{"output", final_path.filename().string()}, {"input_width", iw}, {"input_height", ih},
          {"width", ow}, {"height", oh}, {"bytes", static_cast<long>(fs::file_size(final_path))}};
}

// ============================== generate_report =======================================
namespace {
std::string csv_field(const std::string& s) {
  if (s.find_first_of(",\"\n") == std::string::npos) return s;
  std::string o = "\"";
  for (char c : s) { if (c == '"') o += '"'; o += c; }
  return o + "\"";
}
}  // namespace

json handle_generate_report(JobContext& ctx, const json& p) {
  reject_unknown(p, {"report_type", "days", "format"});
  if (get_str(p, "report_type", std::string("job_summary")) != "job_summary")
    throw PermanentError("invalid payload: unsupported report_type");
  const int days = static_cast<int>(get_int(p, "days", 1, 365, 7));
  const std::string format = get_str(p, "format", std::string("csv"));
  if (format != "csv" && format != "json") throw PermanentError("invalid payload: 'format' must be csv or json");

  db::Connection conn(ctx.dsn);  // reports read the queue itself; never touches other data
  auto r = conn.exec(
      "SELECT task_type, status, count(*)::int, "
      "  COALESCE(round(avg(EXTRACT(EPOCH FROM (finished_at - started_at)) * 1000))::int, 0) "
      "FROM jobs WHERE created_at >= now() - make_interval(days => $1::int) AND id <> $2::uuid "
      "GROUP BY task_type, status ORDER BY task_type, status",
      {std::to_string(days), ctx.job_id});
  ctx.info("report rows: " + std::to_string(r.rows()));

  const fs::path final_path = output_path(ctx, format);
  const fs::path tmp = tmp_of(final_path);
  {
    std::ofstream f(tmp, std::ios::trunc);
    if (!f) throw TemporaryError("could not open report output");
    if (format == "csv") {
      f << "task_type,status,jobs,avg_runtime_ms\n";
      for (int i = 0; i < r.rows(); ++i)
        f << csv_field(r.str(i, 0)) << ',' << csv_field(r.str(i, 1)) << ',' << r.str(i, 2) << ',' << r.str(i, 3) << '\n';
    } else {
      json rows = json::array();
      for (int i = 0; i < r.rows(); ++i)
        rows.push_back({{"task_type", r.str(i, 0)}, {"status", r.str(i, 1)}, {"jobs", r.integer(i, 2)},
                        {"avg_runtime_ms", r.integer(i, 3)}});
      f << json{{"days", days}, {"rows", rows}}.dump(2) << '\n';
    }
    if (!f) throw TemporaryError("could not write report output");
  }
  fs::rename(tmp, final_path);
  return {{"output", final_path.filename().string()}, {"rows", r.rows()}, {"days", days}};
}

// ============================== flaky_api =============================================
json handle_flaky_api(JobContext& ctx, const json& p) {
  reject_unknown(p, {"endpoint", "failure_rate", "latency_ms", "fail_first_n_attempts", "permanent"});
  const std::string endpoint = get_str(p, "endpoint");
  static const std::regex ep(R"(^[A-Za-z0-9/_.-]{1,100}$)");
  if (!std::regex_match(endpoint, ep)) throw PermanentError("invalid payload: bad 'endpoint'");
  double rate = 0.5;
  if (auto it = p.find("failure_rate"); it != p.end()) {
    if (!it->is_number()) throw PermanentError("invalid payload: 'failure_rate' must be a number");
    rate = it->get<double>();
    if (rate < 0.0 || rate > 1.0) throw PermanentError("invalid payload: 'failure_rate' out of range");
  }
  const int latency = static_cast<int>(get_int(p, "latency_ms", 0, kMaxLatencyMs, 100));
  const int fail_first = static_cast<int>(get_int(p, "fail_first_n_attempts", 0, 10, 0));
  const bool permanent = get_bool(p, "permanent", false);

  ctx.info("calling simulated API " + endpoint + " (attempt " + std::to_string(ctx.attempt) + ")");
  std::this_thread::sleep_for(std::chrono::milliseconds(latency));

  thread_local std::mt19937_64 rng{std::random_device{}()};
  bool fail = ctx.attempt <= fail_first || std::uniform_real_distribution<double>(0.0, 1.0)(rng) < rate;
  if (fail) {
    if (permanent) throw PermanentError("HTTP 400 Bad Request from " + endpoint);
    throw TemporaryError("HTTP 503 Service Unavailable from " + endpoint);
  }
  return {{"endpoint", endpoint}, {"http_status", 200}, {"latency_ms", latency}, {"attempt", ctx.attempt}};
}

void register_builtin_handlers(Scheduler& s) {
  s.register_handler("image_resize", handle_image_resize);
  s.register_handler("generate_report", handle_generate_report);
  s.register_handler("flaky_api", handle_flaky_api);
}

}  // namespace taskforge
