from taskqueue.worker.worker import Worker

from tests.test_worker import enqueue, get_job


def test_claim_sets_lease(conn):
    job_id = enqueue(conn, "sleep")
    w = Worker(worker_id="w-lease", lease_seconds=30)
    job = w.claim()
    assert job["id"] == job_id
    remaining = conn.execute(
        "SELECT lease_expires_at - now() FROM jobs WHERE id = %s", (job_id,)
    ).fetchone()[0]
    assert 25 < remaining.total_seconds() <= 30


def test_heartbeat_registers_worker_and_extends_lease(conn):
    job_id = enqueue(conn, "sleep")
    w = Worker(worker_id="w-hb", lease_seconds=30)
    w.claim()

    # simulate time passing: pull the lease back to near expiry
    conn.execute(
        "UPDATE jobs SET lease_expires_at = now() + interval '1 second' WHERE id = %s",
        (job_id,),
    )
    w.heartbeat()

    remaining = conn.execute(
        "SELECT lease_expires_at - now() FROM jobs WHERE id = %s", (job_id,)
    ).fetchone()[0]
    assert remaining.total_seconds() > 25

    hb = conn.execute(
        "SELECT last_heartbeat FROM workers WHERE id = 'w-hb'"
    ).fetchone()
    assert hb is not None


def test_heartbeat_only_touches_own_jobs(conn):
    mine = enqueue(conn, "sleep")
    theirs = enqueue(conn, "sleep")
    me, other = Worker(worker_id="me"), Worker(worker_id="other")
    assert me.claim()["id"] == mine
    assert other.claim()["id"] == theirs

    conn.execute("UPDATE jobs SET lease_expires_at = now()")
    me.heartbeat()

    rows = dict(
        conn.execute(
            "SELECT claimed_by, lease_expires_at > now() FROM jobs"
        ).fetchall()
    )
    assert rows == {"me": True, "other": False}
