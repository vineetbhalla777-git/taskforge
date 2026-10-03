#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <string>

#include "taskforge/metrics_server.hpp"

using namespace taskforge;

namespace {
// Minimal blocking HTTP GET over a raw socket, just enough to exercise the server.
std::string http_get(int port, const std::string& path) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
  if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr) < 0) { close(fd); return ""; }
  std::string req = "GET " + path + " HTTP/1.1\r\nHost: localhost\r\n\r\n";
  send(fd, req.data(), req.size(), 0);
  std::string resp;
  char buf[4096];
  ssize_t n;
  while ((n = recv(fd, buf, sizeof buf, 0)) > 0) resp.append(buf, static_cast<size_t>(n));
  close(fd);
  return resp;
}
int free_port() {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = 0; a.sin_addr.s_addr = INADDR_ANY;
  bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof a);
  socklen_t len = sizeof a;
  getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
  int p = ntohs(a.sin_port);
  close(fd);
  return p;
}
}  // namespace

TEST(MetricsServer, ServesMetricsBody) {
  int port = free_port();
  MetricsServer srv(port, [] { return "taskforge_jobs_succeeded_total 42\n"; });
  auto resp = http_get(port, "/metrics");
  EXPECT_NE(resp.find("HTTP/1.1 200"), std::string::npos);
  EXPECT_NE(resp.find("Content-Type: text/plain"), std::string::npos);
  EXPECT_NE(resp.find("taskforge_jobs_succeeded_total 42"), std::string::npos);
}

TEST(MetricsServer, HealthzAnd404) {
  int port = free_port();
  MetricsServer srv(port, [] { return "x"; });
  EXPECT_NE(http_get(port, "/healthz").find("200 OK"), std::string::npos);
  EXPECT_NE(http_get(port, "/nope").find("404"), std::string::npos);
}

TEST(MetricsServer, RendererCalledFreshEachRequest) {
  int port = free_port();
  int calls = 0;
  MetricsServer srv(port, [&] { return "calls=" + std::to_string(++calls); });
  auto r1 = http_get(port, "/metrics");
  auto r2 = http_get(port, "/metrics");
  EXPECT_NE(r1.find("calls=1"), std::string::npos);
  EXPECT_NE(r2.find("calls=2"), std::string::npos);
}

TEST(MetricsServer, HandlesSequentialRequestsWithoutHanging) {
  int port = free_port();
  MetricsServer srv(port, [] { return "ok\n"; });
  for (int i = 0; i < 20; ++i) EXPECT_NE(http_get(port, "/metrics").find("200"), std::string::npos);
}

TEST(MetricsServer, ThrowsOnPortConflict) {
  int port = free_port();
  MetricsServer first(port, [] { return "x"; });
  EXPECT_THROW(MetricsServer(port, [] { return "y"; }), std::runtime_error);
}
