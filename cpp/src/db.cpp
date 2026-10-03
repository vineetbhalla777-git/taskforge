#include "taskforge/db.hpp"

#include <cstdlib>

namespace taskforge::db {

void Connection::connect() {
  if (conn_) { PQfinish(conn_); conn_ = nullptr; }
  conn_ = PQconnectdb(dsn_.c_str());
  if (!conn_ || PQstatus(conn_) != CONNECTION_OK) {
    std::string msg = conn_ ? PQerrorMessage(conn_) : "out of memory";
    if (conn_) { PQfinish(conn_); conn_ = nullptr; }
    throw Error("postgres connect failed: " + msg);
  }
}

Result Connection::exec(const std::string& sql, const Params& params) {
  // If the server dropped us while idle (restart, failover), reconnect once.
  // Never do this mid-transaction: PQstatus stays OK there, so a lost
  // transaction surfaces as an error from PQexecParams instead.
  if (PQstatus(conn_) != CONNECTION_OK) connect();

  std::vector<const char*> values;
  values.reserve(params.size());
  for (const auto& p : params) values.push_back(p ? p->c_str() : nullptr);

  PGresult* r = PQexecParams(conn_, sql.c_str(), static_cast<int>(params.size()), nullptr,
                             values.empty() ? nullptr : values.data(), nullptr, nullptr, 0);
  if (!r) throw Error(std::string("postgres: ") + PQerrorMessage(conn_));
  ExecStatusType st = PQresultStatus(r);
  if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
    std::string msg = PQresultErrorMessage(r);
    PQclear(r);
    throw Error("postgres: " + msg);
  }
  return Result(r);
}

}  // namespace taskforge::db
