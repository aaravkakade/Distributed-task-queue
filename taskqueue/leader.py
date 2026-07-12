"""Leader election on a Postgres session-level advisory lock.

Exactly one session can hold pg_advisory_lock(KEY) at a time, and Postgres
releases it automatically when the holder's session ends — so a crashed
leader (kill -9, network partition closing the TCP connection) is deposed
without any timeout bookkeeping on our side. Standbys just keep calling
pg_try_advisory_lock (non-blocking) until they win.

The lock must be taken on a connection that stays open for the whole term of
leadership; losing the connection means losing the lock.
"""

import psycopg

# arbitrary but fixed application-wide key ("TQ" in hex, padded)
REAPER_LEADER_KEY = 0x5451_0001


class LeaderLock:
    def __init__(self, conn: psycopg.Connection, key: int = REAPER_LEADER_KEY):
        self.conn = conn
        self.key = key

    def try_acquire(self) -> bool:
        row = self.conn.execute(
            "SELECT pg_try_advisory_lock(%s)", (self.key,)
        ).fetchone()
        got = row[0] if not isinstance(row, dict) else row["pg_try_advisory_lock"]
        return bool(got)

    def release(self) -> None:
        self.conn.execute("SELECT pg_advisory_unlock(%s)", (self.key,))
