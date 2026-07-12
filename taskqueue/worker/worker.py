"""Worker: claims queued jobs with SELECT ... FOR UPDATE SKIP LOCKED and runs them.

The claim is a single UPDATE wrapping a locking subquery, so under any number of
concurrent workers each queued job is handed to exactly one of them: SKIP LOCKED
makes competing claimants skip rows another transaction has already locked
instead of blocking on them.
"""

import logging
import os
import socket
import threading
import time
import traceback
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from taskqueue import tasks
from taskqueue.config import (
    DATABASE_URL,
    HEARTBEAT_INTERVAL,
    LEASE_SECONDS,
    POLL_INTERVAL,
)

log = logging.getLogger("taskqueue.worker")

CLAIM_SQL = """
UPDATE jobs
   SET state = 'running',
       claimed_by = %(worker_id)s,
       attempts = attempts + 1,
       started_at = now(),
       lease_expires_at = now() + %(lease)s * interval '1 second',
       updated_at = now()
 WHERE id = (
        SELECT id FROM jobs
         WHERE state = 'queued' AND run_at <= now()
         ORDER BY priority DESC, run_at, id
         FOR UPDATE SKIP LOCKED
         LIMIT 1
       )
RETURNING *
"""


class Worker:
    def __init__(
        self,
        dsn: str = DATABASE_URL,
        worker_id: str | None = None,
        lease_seconds: float = LEASE_SECONDS,
    ):
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}"
        self.dsn = dsn
        self.lease_seconds = lease_seconds
        self.conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)
        self.stopping = threading.Event()

    def claim(self) -> dict | None:
        return self.conn.execute(
            CLAIM_SQL, {"worker_id": self.worker_id, "lease": self.lease_seconds}
        ).fetchone()

    def heartbeat(self, conn: psycopg.Connection | None = None) -> None:
        """One beat: refresh worker liveness and extend leases on our running jobs.

        The lease, not the workers row, is what protects in-flight jobs — the
        reaper re-queues any running job whose lease lapses, so a worker that
        dies mid-task (kill -9, network partition) simply stops extending it.
        """
        conn = conn or self.conn
        conn.execute(
            """INSERT INTO workers (id) VALUES (%s)
               ON CONFLICT (id) DO UPDATE SET last_heartbeat = now()""",
            (self.worker_id,),
        )
        conn.execute(
            """UPDATE jobs
                  SET lease_expires_at = now() + %s * interval '1 second',
                      updated_at = now()
                WHERE claimed_by = %s AND state = 'running'""",
            (self.lease_seconds, self.worker_id),
        )

    def _heartbeat_loop(self) -> None:
        # Own connection: the main connection is busy while a task executes.
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            while not self.stopping.wait(HEARTBEAT_INTERVAL):
                try:
                    self.heartbeat(conn)
                except psycopg.Error:
                    log.exception("heartbeat failed")

    def execute(self, job: dict) -> None:
        try:
            fn = tasks.REGISTRY.get(job["task_name"])
            if fn is None:
                raise LookupError(f"unknown task: {job['task_name']}")
            result = fn(**job["payload"], _attempt=job["attempts"])
        except Exception as exc:
            self._finish_failed(job, exc)
        else:
            self._finish_succeeded(job, result)

    def _finish_succeeded(self, job: dict, result: Any) -> None:
        self.conn.execute(
            """UPDATE jobs
                  SET state = 'succeeded', result = %s, finished_at = now(),
                      updated_at = now()
                WHERE id = %s AND state = 'running' AND claimed_by = %s""",
            (Jsonb(result), job["id"], self.worker_id),
        )
        log.info("job %s succeeded", job["id"])

    def _finish_failed(self, job: dict, exc: Exception) -> None:
        error = "".join(traceback.format_exception_only(exc)).strip()
        self.conn.execute(
            """UPDATE jobs
                  SET state = 'failed', last_error = %s, finished_at = now(),
                      updated_at = now()
                WHERE id = %s AND state = 'running' AND claimed_by = %s""",
            (error, job["id"], self.worker_id),
        )
        log.warning("job %s failed: %s", job["id"], error)

    def run_once(self) -> bool:
        """Claim and execute one job. Returns False if the queue was empty."""
        job = self.claim()
        if job is None:
            return False
        log.info("claimed job %s (%s), attempt %s",
                 job["id"], job["task_name"], job["attempts"])
        self.execute(job)
        return True

    def run_forever(self) -> None:
        log.info("worker %s starting", self.worker_id)
        self.heartbeat()
        hb = threading.Thread(target=self._heartbeat_loop, daemon=True)
        hb.start()
        try:
            while not self.stopping.is_set():
                if not self.run_once():
                    time.sleep(POLL_INTERVAL)
        finally:
            self.stopping.set()
            hb.join(timeout=HEARTBEAT_INTERVAL + 1)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    Worker().run_forever()
