#include "taskforge/metrics_server.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <stdexcept>

namespace taskforge {
namespace {
constexpr int kMaxRequestLine = 8192;
constexpr int kAcceptTimeoutMs = 200;  // lets the accept loop notice stop_ promptly
}  // namespace

MetricsServer::MetricsServer(int port, Renderer renderer) : port_(port), renderer_(std::move(renderer)) {
  listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) throw std::runtime_error("metrics server: socket() failed");
  int one = 1;
  setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof addr) < 0) {
    ::close(listen_fd_);
    throw std::runtime_error("metrics server: bind() failed on port " + std::to_string(port));
  }
  if (::listen(listen_fd_, 16) < 0) {
    ::close(listen_fd_);
    throw std::runtime_error("metrics server: listen() failed");
  }
  timeval tv{0, kAcceptTimeoutMs * 1000};
  setsockopt(listen_fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
  thread_ = std::thread([this] { accept_loop(); });
}

MetricsServer::~MetricsServer() {
  stop_ = true;
  if (listen_fd_ >= 0) ::shutdown(listen_fd_, SHUT_RDWR);
  if (thread_.joinable()) thread_.join();
  if (listen_fd_ >= 0) ::close(listen_fd_);
}

void MetricsServer::accept_loop() {
  while (!stop_) {
    sockaddr_in peer{};
    socklen_t len = sizeof peer;
    int fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &len);
    if (fd < 0) continue;  // timeout or transient error: loop back and check stop_
    handle(fd);
    ::close(fd);
  }
}

void MetricsServer::handle(int client_fd) {
  char buf[kMaxRequestLine];
  ssize_t n = ::recv(client_fd, buf, sizeof(buf) - 1, 0);
  if (n <= 0) return;
  buf[n] = '\0';
  std::string req(buf, static_cast<size_t>(n));
  std::string path = "/";
  auto sp1 = req.find(' ');
  auto sp2 = (sp1 == std::string::npos) ? std::string::npos : req.find(' ', sp1 + 1);
  if (sp1 != std::string::npos && sp2 != std::string::npos) path = req.substr(sp1 + 1, sp2 - sp1 - 1);

  std::string body, status = "200 OK", content_type = "text/plain; version=0.0.4";
  if (path == "/metrics") {
    body = renderer_();
  } else if (path == "/healthz") {
    body = "ok\n";
  } else {
    status = "404 Not Found";
    body = "not found\n";
  }
  std::string resp = "HTTP/1.1 " + status + "\r\nContent-Type: " + content_type +
                     "\r\nContent-Length: " + std::to_string(body.size()) +
                     "\r\nConnection: close\r\n\r\n" + body;
  ::send(client_fd, resp.data(), resp.size(), 0);
}

}  // namespace taskforge
