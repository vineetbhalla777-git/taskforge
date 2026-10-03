from __future__ import annotations

from pathlib import Path

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DEFAULT_SCHEMA = Path(__file__).resolve().parents[1] / "db" / "schema.sql"


def make_pool(dsn: str, max_size: int) -> ConnectionPool:
    return ConnectionPool(dsn, min_size=1, max_size=max_size, kwargs={"row_factory": dict_row},
                          open=False, timeout=10)


def apply_schema(pool: ConnectionPool, path: Path = DEFAULT_SCHEMA) -> None:
    """Idempotent. The advisory lock serialises concurrent API/scheduler start-ups."""
    sql = Path(path).read_text()
    with pool.connection() as conn:
        conn.execute("SELECT pg_advisory_lock(727274)")
        try:
            conn.execute(sql)
            conn.commit()
        finally:
            conn.execute("SELECT pg_advisory_unlock(727274)")
            conn.commit()
