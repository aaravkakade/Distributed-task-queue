import os
import pathlib
import signal
import subprocess
import sys
import time

from taskqueue.config import DATABASE_URL
from taskqueue.reaper import Reaper
from taskqueue.worker.worker import Worker

from tests.test_worker import enqueue, get_job

REPO_ROOT = pathlib.Path(__file__).parent.parent


def expire_lease(conn, job_id):
    conn.execute(
        "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = %s",
        (job_id,),
    )


def test_reaper_requeues_expired_lease(conn):
    job_id = enqueue(conn, "sleep")
    Worker(worker_id="w-dead").claim()
    expire_lease(conn, job_id)

    requeued, buried = Reaper().reap_once()
    assert requeued == [job_id] and buried == []

    job = get_job(conn, job_id)
    assert job["state"] == "queued"
    assert job["claimed_by"] is None and job["lease_expires_at"] is None
    assert job["attempts"] == 1  # the burned attempt stays counted
    assert "presumed dead" in job["last_error"]


def test_reaper_buries_exhausted_job(conn):
    job_id = enqueue(conn, "sleep", max_attempts=1)
    Worker(worker_id="w-dead").claim()
    expire_lease(conn, job_id)

    requeued, buried = Reaper().reap_once()
    assert requeued == [] and buried == [job_id]
    assert get_job(conn, job_id)["state"] == "dead"


def test_reaper_leaves_live_leases_alone(conn):
    job_id = enqueue(conn, "sleep")
    Worker(worker_id="w-alive", lease_seconds=60).claim()

    assert Reaper().reap_once() == ([], [])
    assert get_job(conn, job_id)["state"] == "running"


def wait_for(predicate, timeout=10, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_kill9_recovery_end_to_end(conn):
    """The headline failure drill: kill -9 a worker mid-job, measure recovery.

    A real worker subprocess claims a job and is SIGKILLed while running it.
    Its lease (1s here) lapses, the reaper re-queues the job, and a second
    worker completes it — at-least-once delivery, no operator involved.
    """
    job_id = enqueue(conn, "sleep", {"seconds": 30})

    env = os.environ | {
        "DATABASE_URL": DATABASE_URL,
        "LEASE_SECONDS": "1",
        "HEARTBEAT_INTERVAL": "0.25",
        "POLL_INTERVAL": "0.1",
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "taskqueue.worker.worker"],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        assert wait_for(
            lambda: get_job(conn, job_id)["state"] == "running"
        ), "worker never claimed the job"

        proc.send_signal(signal.SIGKILL)
        proc.wait()
        killed_at = time.monotonic()

        reaper = Reaper()
        assert wait_for(
            lambda: reaper.reap_once() and get_job(conn, job_id)["state"] == "queued"
        ), "job was never re-queued after worker death"
        recovery_seconds = time.monotonic() - killed_at
        print(f"\nrecovery after kill -9: {recovery_seconds:.2f}s")

        # a healthy worker picks the job up and finishes it
        conn.execute(
            "UPDATE jobs SET payload = '{\"seconds\": 0.1}' WHERE id = %s", (job_id,)
        )
        assert Worker(worker_id="w-rescue").run_once() is True
        job = get_job(conn, job_id)
        assert job["state"] == "succeeded"
        assert job["attempts"] == 2  # first attempt died with the worker
        assert recovery_seconds < 5
    finally:
        if proc.poll() is None:
            proc.kill()
