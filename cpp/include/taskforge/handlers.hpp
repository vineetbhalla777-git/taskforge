#pragma once
#include "taskforge/handler.hpp"

namespace taskforge {
class Scheduler;

// Resource limits (mirrored in api/app/tasks.py).
constexpr int kMaxImageDim = 4096;
constexpr long kMaxInputPixels = 25'000'000;  // ~100 MB decoded RGBA
constexpr int kMaxLatencyMs = 5000;

nlohmann::json handle_image_resize(JobContext&, const nlohmann::json& payload);
nlohmann::json handle_generate_report(JobContext&, const nlohmann::json& payload);
nlohmann::json handle_flaky_api(JobContext&, const nlohmann::json& payload);

void register_builtin_handlers(Scheduler&);

}  // namespace taskforge
