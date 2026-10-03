// A tiny embedded HTTP server exposing scheduler counters in Prometheus text format,
// with no third-party dependency (raw POSIX sockets). It only ever serves GET /metrics
// (and /healthz) with a fixed, pre-rendered body -- there is no request parsing surface
// worth mentioning, but it does bound the request line length it will read before
// giving up, so a malformed client cannot hang a handler thread indefinitely.
#pragma once
#include <atomic>
#include <functional>
#include <string>
#include <thread>

namespace taskforge {

class MetricsServer {
 public:
  using Renderer = std::function<std::string()>;  // returns the full Prometheus exposition text

  // Binds immediately; throws std::runtime_error if the port is unavailable.
  MetricsServer(int port, Renderer renderer);
  ~MetricsServer();
  MetricsServer(const MetricsServer&) = delete;
  MetricsServer& operator=(const MetricsServer&) = delete;

  int port() const { return port_; }

 private:
  void accept_loop();
  void handle(int client_fd);

  int listen_fd_ = -1;
  int port_;
  Renderer renderer_;
  std::atomic<bool> stop_{false};
  std::thread thread_;
};

}  // namespace taskforge
