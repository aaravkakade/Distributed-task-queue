"""Database access: connection pool + migration runner."""

import pathlib

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from taskqueue.config import DATABASE_URL

MIGRATIONS_DIR = pathlib.Path(__file__).parent / "migrations"

_pool: ConnectionPool | None = None


def get_pool(dsn: str = DATABASE_URL) -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(dsn, min_size=1, max_size=10, open=True,
                               kwargs={"row_factory": dict_row})
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


def migrate(dsn: str = DATABASE_URL) -> list[str]:
    """Apply pending migrations in filename order. Returns the ones applied."""
    applied: list[str] = []
    with psycopg.connect(dsn) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS schema_migrations (
                   name TEXT PRIMARY KEY,
                   applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
               )"""
        )
        done = {
            r[0] for r in conn.execute("SELECT name FROM schema_migrations").fetchall()
        }
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            if path.name in done:
                continue
            conn.execute(path.read_text())
            conn.execute(
                "INSERT INTO schema_migrations (name) VALUES (%s)", (path.name,)
            )
            applied.append(path.name)
        conn.commit()
    return applied


if __name__ == "__main__":
    names = migrate()
    print(f"applied: {names}" if names else "up to date")
