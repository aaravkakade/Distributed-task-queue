from taskqueue.reaper import Reaper
from taskqueue.worker.worker import Worker

from tests.test_reaper import expire_lease, wait_for
from tests.test_worker import enqueue, get_job


def test_only_one_leader_at_a_time(conn):
    r1, r2 = Reaper(), Reaper()
    assert r1.step() is True
    assert r2.step() is False
    assert r2.step() is False  # still standby on subsequent ticks


def test_standby_does_not_reap(conn):
    r1, r2 = Reaper(), Reaper()
    assert r1.step() is True

    job_id = enqueue(conn, "sleep")
    Worker(worker_id="w-dead").claim()
    expire_lease(conn, job_id)

    # standby ticks but must not touch the expired job
    r2.is_leader = r2.lock.try_acquire()
    assert r2.is_leader is False
    assert get_job(conn, job_id)["state"] == "running"

    r1.step()  # leader reaps it
    assert get_job(conn, job_id)["state"] == "queued"


def test_failover_when_leader_dies(conn):
    leader, standby = Reaper(), Reaper()
    assert leader.step() is True
    assert standby.step() is False

    # leader "crashes": its session ends, Postgres releases the advisory lock
    leader.conn.close()

    assert wait_for(standby.step, timeout=5), "standby never took over"

    # and the new leader actually does the job
    job_id = enqueue(conn, "sleep")
    Worker(worker_id="w-dead").claim()
    expire_lease(conn, job_id)
    standby.step()
    assert get_job(conn, job_id)["state"] == "queued"
