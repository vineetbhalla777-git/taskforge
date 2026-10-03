// Thin RAII wrapper over libpq. One Connection is NOT thread-safe: give each
// thread (scheduler, each heartbeat/reaper loop, tests) its own.
#pragma once
#include <libpq-fe.h>

#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace taskforge::db {

class Error : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

using Param = std::optional<std::string>;  // nullopt => SQL NULL
using Params = std::vector<Param>;

class Result {
 public:
  explicit Result(PGresult* r) : r_(r) {}
  ~Result() { if (r_) PQclear(r_); }
  Result(Result&& o) noexcept : r_(o.r_) { o.r_ = nullptr; }
  Result& operator=(Result&& o) noexcept {
    if (this != &o) { if (r_) PQclear(r_); r_ = o.r_; o.r_ = nullptr; }
    return *this;
  }
  Result(const Result&) = delete;
  Result& operator=(const Result&) = delete;

  int rows() const { return PQntuples(r_); }
  bool is_null(int row, int col) const { return PQgetisnull(r_, row, col) != 0; }
  std::string str(int row, int col) const { return PQgetisnull(r_, row, col) ? "" : PQgetvalue(r_, row, col); }
  int integer(int row, int col) const { return std::stoi(str(row, col)); }
  int affected() const { const char* s = PQcmdTuples(r_); return (s && *s) ? std::atoi(s) : 0; }

 private:
  PGresult* r_;
};

class Connection {
 public:
  explicit Connection(const std::string& dsn) : dsn_(dsn) { connect(); }
  ~Connection() { if (conn_) PQfinish(conn_); }
  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  // Runs one statement with $1..$n parameters (sent as text; use casts such as
  // $1::uuid in SQL). Reconnects once if the connection was lost between calls.
  Result exec(const std::string& sql, const Params& params = {});

  void begin() { exec("BEGIN"); }
  void commit() { exec("COMMIT"); }
  void rollback() noexcept { try { exec("ROLLBACK"); } catch (...) {} }

 private:
  void connect();
  PGconn* conn_ = nullptr;
  std::string dsn_;
};

}  // namespace taskforge::db
