"""Prove that concurrent workers never double-claim a job.

Eight workers (each with its own DB connection, racing at the Postgres level)
drain a queue of 300 jobs. If SKIP LOCKED claiming were broken, the same job
would be executed by two workers — visible as a duplicate id across the
per-worker claim logs, or as attempts > 1.
"""

import threading
from collections import Counter

from psycopg.types.json import Jsonb

from taskqueue.worker.worker import Worker

N_JOBS = 300
N_WORKERS = 8


def test_no_double_claim_under_load(conn):
    conn.execute(
        "INSERT INTO jobs (task_name, payload) SELECT 'echo', %s FROM generate_series(1, %s)",
        (Jsonb({}), N_JOBS),
    )

    claimed: dict[str, list[int]] = {}

    def drain(worker_id: str):
        w = Worker(worker_id=worker_id)
        ids = claimed.setdefault(worker_id, [])
        while True:
            job = w.claim()
            if job is None:
                break
            ids.append(job["id"])
            w.execute(job)
        w.conn.close()

    threads = [
        threading.Thread(target=drain, args=(f"w{i}",)) for i in range(N_WORKERS)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    all_ids = [job_id for ids in claimed.values() for job_id in ids]
    dupes = [job_id for job_id, n in Counter(all_ids).items() if n > 1]
    assert dupes == [], f"jobs claimed more than once: {dupes}"
    assert len(all_ids) == N_JOBS  # nothing lost, nothing left behind

    bad = conn.execute(
        "SELECT count(*) FROM jobs WHERE state != 'succeeded' OR attempts != 1"
    ).fetchone()[0]
    assert bad == 0

    # real contention: the work was actually spread across workers
    assert sum(1 for ids in claimed.values() if ids) >= 2
