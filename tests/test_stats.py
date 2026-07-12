import pytest
from fastapi.testclient import TestClient

from taskqueue.coordinator.app import app
from taskqueue.worker.worker import Worker

from tests.test_worker import enqueue


@pytest.fixture()
def client(conn):
    with TestClient(app) as c:
        yield c


def test_stats_counts_states_and_workers(client, conn):
    enqueue(conn, "echo")
    enqueue(conn, "echo")
    done_id = enqueue(conn, "fail", max_attempts=1)

    w = Worker(worker_id="w-stats", backoff_base=0)
    w.heartbeat()
    # claim order is FIFO, so pull the fail job to the front
    conn.execute("UPDATE jobs SET priority = 10 WHERE id = %s", (done_id,))
    w.run_once()  # fail job -> dead (max_attempts=1)
    w.run_once()  # first echo -> succeeded

    stats = client.get("/stats").json()
    assert stats["jobs"]["queued"] == 1
    assert stats["jobs"]["succeeded"] == 1
    assert stats["jobs"]["dead"] == 1
    assert stats["live_workers"] == 1
    assert stats["succeeded_last_minute"] == 1
    assert stats["oldest_ready_seconds"] >= 0


def test_stats_empty_queue(client):
    stats = client.get("/stats").json()
    assert stats["jobs"]["queued"] == 0
    assert stats["oldest_ready_seconds"] is None
