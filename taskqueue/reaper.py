"""Reaper: failure detector. Re-queues in-flight jobs whose lease expired.

A worker that dies (kill -9, OOM, network partition) stops renewing its leases.
Once a running job's lease lapses, the reaper puts it back in 'queued' so any
live worker can pick it up — this is what makes delivery *at-least-once*: the
old worker may have partially executed the job, so tasks must be idempotent.

Jobs that have already burned max_attempts go to 'dead' instead of looping
forever (a poison pill that crashes workers would otherwise cycle endlessly).
"""

import logging
import time

import psycopg
from psycopg.rows import dict_row

from taskqueue.config import DATABASE_URL, HEARTBEAT_INTERVAL, REAP_INTERVAL

log = logging.getLogger("taskqueue.reaper")

REQUEUE_SQL = """
UPDATE jobs
   SET state = 'queued',
       claimed_by = NULL,
       lease_expires_at = NULL,
       started_at = NULL,
       last_error = format('lease expired (worker %s presumed dead)', claimed_by),
       updated_at = now()
 WHERE state = 'running' AND lease_expires_at < now() AND attempts < max_attempts
RETURNING id
"""

BURY_SQL = """
UPDATE jobs
   SET state = 'dead',
       last_error = format('lease expired (worker %s presumed dead); retries exhausted',
                           claimed_by),
       finished_at = now(),
       updated_at = now()
 WHERE state = 'running' AND lease_expires_at < now() AND attempts >= max_attempts
RETURNING id
"""


class Reaper:
    def __init__(self, dsn: str = DATABASE_URL):
        self.conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)
        self.stopping = False

    def reap_once(self) -> tuple[list[int], list[int]]:
        """Returns (requeued job ids, buried job ids)."""
        buried = [r["id"] for r in self.conn.execute(BURY_SQL).fetchall()]
        requeued = [r["id"] for r in self.conn.execute(REQUEUE_SQL).fetchall()]

        # housekeeping: drop workers rows that stopped heartbeating long ago
        self.conn.execute(
            "DELETE FROM workers WHERE last_heartbeat < now() - %s * interval '1 second'",
            (10 * HEARTBEAT_INTERVAL,),
        )

        if requeued:
            log.warning("re-queued expired jobs: %s", requeued)
        if buried:
            log.warning("buried exhausted jobs (dead-letter): %s", buried)
        return requeued, buried

    def run_forever(self) -> None:
        log.info("reaper starting (interval %ss)", REAP_INTERVAL)
        while not self.stopping:
            self.reap_once()
            time.sleep(REAP_INTERVAL)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    Reaper().run_forever()
