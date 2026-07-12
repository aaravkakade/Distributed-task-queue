"""Measured throughput benchmark. TRUNCATES the jobs table — dev use only.

    python scripts/benchmark.py [n_jobs] [n_workers]

Enqueues n_jobs no-op ('echo') jobs, launches n_workers real worker
subprocesses, and measures wall-clock time from worker launch until every job
has succeeded. Reported jobs/sec therefore includes claim contention, task
dispatch, and the success write — the full queue round-trip minus task work.
"""

import os
import subprocess
import sys
import time

import psycopg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from taskqueue.config import DATABASE_URL  # noqa: E402


def main(n_jobs: int = 2000, n_workers: int = 4) -> None:
    conn = psycopg.connect(DATABASE_URL, autocommit=True)
    conn.execute("TRUNCATE jobs, workers RESTART IDENTITY")
    conn.execute(
        "INSERT INTO jobs (task_name) SELECT 'echo' FROM generate_series(1, %s)",
        (n_jobs,),
    )

    env = os.environ | {"DATABASE_URL": DATABASE_URL, "POLL_INTERVAL": "0.05"}
    t0 = time.monotonic()
    workers = [
        subprocess.Popen(
            [sys.executable, "-m", "taskqueue.worker.worker"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        for _ in range(n_workers)
    ]
    try:
        while True:
            done = conn.execute(
                "SELECT count(*) FROM jobs WHERE state = 'succeeded'"
            ).fetchone()[0]
            if done == n_jobs:
                break
            time.sleep(0.05)
        elapsed = time.monotonic() - t0
    finally:
        for w in workers:
            w.kill()

    print(f"{n_jobs} jobs, {n_workers} workers: "
          f"{elapsed:.2f}s -> {n_jobs / elapsed:.0f} jobs/sec")


if __name__ == "__main__":
    main(
        int(sys.argv[1]) if len(sys.argv) > 1 else 2000,
        int(sys.argv[2]) if len(sys.argv) > 2 else 4,
    )
