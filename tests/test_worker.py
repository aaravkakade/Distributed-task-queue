from psycopg.types.json import Jsonb

from taskqueue.worker.worker import Worker


def enqueue(conn, task_name, payload=None, **cols):
    keys = ["task_name", "payload", *cols]
    vals = [task_name, Jsonb(payload or {}), *cols.values()]
    placeholders = ", ".join(["%s"] * len(vals))
    return conn.execute(
        f"INSERT INTO jobs ({', '.join(keys)}) VALUES ({placeholders}) RETURNING id",
        vals,
    ).fetchone()[0]


def get_job(conn, job_id):
    cur = conn.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
    cols = [d.name for d in cur.description]
    return dict(zip(cols, cur.fetchone()))


def test_worker_runs_job_to_success(conn):
    job_id = enqueue(conn, "echo", {"msg": "hi"})
    w = Worker(worker_id="w-test")
    assert w.run_once() is True

    job = get_job(conn, job_id)
    assert job["state"] == "succeeded"
    assert job["result"] == {"msg": "hi"}
    assert job["attempts"] == 1
    assert job["claimed_by"] == "w-test"
    assert job["started_at"] is not None and job["finished_at"] is not None


def test_worker_marks_failure(conn):
    job_id = enqueue(conn, "fail", {"message": "kaput"})
    Worker(worker_id="w-test").run_once()

    job = get_job(conn, job_id)
    assert job["state"] == "failed"
    assert "kaput" in job["last_error"]


def test_unknown_task_fails(conn):
    job_id = enqueue(conn, "no-such-task")
    Worker(worker_id="w-test").run_once()
    assert get_job(conn, job_id)["state"] == "failed"


def test_empty_queue_returns_false(conn):
    assert Worker(worker_id="w-test").run_once() is False


def test_claim_respects_priority_and_run_at(conn):
    low = enqueue(conn, "echo", priority=0)
    high = enqueue(conn, "echo", priority=5)
    future = conn.execute(
        """INSERT INTO jobs (task_name, run_at, priority)
           VALUES ('echo', now() + interval '1 hour', 100) RETURNING id"""
    ).fetchone()[0]

    w = Worker(worker_id="w-test")
    first = w.claim()
    second = w.claim()
    assert [first["id"], second["id"]] == [high, low]
    assert w.claim() is None  # future job is not claimable yet
    assert get_job(conn, future)["state"] == "queued"
