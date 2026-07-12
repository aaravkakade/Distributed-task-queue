import pytest
from fastapi.testclient import TestClient

from taskqueue.coordinator.app import app
from taskqueue.worker.worker import Worker

from tests.test_worker import enqueue, get_job


def drain(worker, max_iters=50):
    for _ in range(max_iters):
        if not worker.run_once():
            break


def test_failed_attempt_schedules_backoff_retry(conn):
    job_id = enqueue(conn, "fail", max_attempts=3)
    Worker(worker_id="w", backoff_base=60).run_once()

    job = get_job(conn, job_id)
    assert job["state"] == "failed"
    assert job["attempts"] == 1
    assert job["claimed_by"] is None
    delay = conn.execute(
        "SELECT run_at - now() FROM jobs WHERE id = %s", (job_id,)
    ).fetchone()[0]
    assert 55 < delay.total_seconds() <= 60  # base * 2^0


def test_backoff_doubles_per_attempt(conn):
    job_id = enqueue(conn, "fail", max_attempts=5)
    w = Worker(worker_id="w", backoff_base=60)
    for expected in (60, 120, 240):  # base * 2^(attempt-1)
        w.run_once()
        delay = conn.execute(
            "SELECT run_at - now() FROM jobs WHERE id = %s", (job_id,)
        ).fetchone()[0]
        assert expected - 5 < delay.total_seconds() <= expected
        # job isn't claimable during backoff; pull run_at back to retry now
        conn.execute(
            "UPDATE jobs SET run_at = now() WHERE id = %s", (job_id,)
        )


def test_backoff_is_capped(conn):
    enqueue(conn, "fail", max_attempts=2)
    w = Worker(worker_id="w", backoff_base=60, backoff_cap=10)
    w.run_once()
    delay = conn.execute("SELECT run_at - now() FROM jobs").fetchone()[0]
    assert delay.total_seconds() <= 10


def test_exhausted_job_goes_dead(conn):
    job_id = enqueue(conn, "fail", max_attempts=2)
    w = Worker(worker_id="w", backoff_base=0)
    drain(w)

    job = get_job(conn, job_id)
    assert job["state"] == "dead"
    assert job["attempts"] == 2
    assert job["finished_at"] is not None


def test_flaky_task_eventually_succeeds(conn):
    job_id = enqueue(conn, "flaky", {"succeed_on_attempt": 3}, max_attempts=5)
    drain(Worker(worker_id="w", backoff_base=0))

    job = get_job(conn, job_id)
    assert job["state"] == "succeeded"
    assert job["attempts"] == 3
    assert job["result"] == {"succeeded_on_attempt": 3}


@pytest.fixture()
def client(conn):
    with TestClient(app) as c:
        yield c


def test_dlq_list_and_retry(client, conn):
    job_id = enqueue(conn, "fail", max_attempts=1)
    drain(Worker(worker_id="w", backoff_base=0))

    dead = client.get("/jobs", params={"state": "dead"}).json()
    assert [j["id"] for j in dead] == [job_id]

    revived = client.post(f"/jobs/{job_id}/retry")
    assert revived.status_code == 200
    assert revived.json()["state"] == "queued"
    assert revived.json()["attempts"] == 0

    assert client.post(f"/jobs/{job_id}/retry").status_code == 409  # not dead now
    assert client.post("/jobs/999999/retry").status_code == 404
