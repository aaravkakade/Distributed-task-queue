import threading

import pytest
from fastapi.testclient import TestClient

from taskqueue.coordinator.app import app


@pytest.fixture()
def client(conn):
    with TestClient(app) as c:
        yield c


def test_same_key_returns_original_job(client):
    first = client.post(
        "/jobs", json={"task_name": "echo", "payload": {"n": 1}, "idempotency_key": "k1"}
    )
    replay = client.post(
        "/jobs", json={"task_name": "echo", "payload": {"n": 1}, "idempotency_key": "k1"}
    )
    assert first.status_code == 201
    assert replay.status_code == 200  # replayed, not re-enqueued
    assert replay.json()["id"] == first.json()["id"]


def test_different_keys_create_different_jobs(client):
    a = client.post("/jobs", json={"task_name": "echo", "idempotency_key": "ka"})
    b = client.post("/jobs", json={"task_name": "echo", "idempotency_key": "kb"})
    assert a.status_code == b.status_code == 201
    assert a.json()["id"] != b.json()["id"]


def test_no_key_never_deduplicates(client):
    a = client.post("/jobs", json={"task_name": "echo"})
    b = client.post("/jobs", json={"task_name": "echo"})
    assert a.json()["id"] != b.json()["id"]


def test_concurrent_double_submit_creates_one_job(client, conn):
    """Simulates a client retrying against two coordinators at once: the
    UNIQUE index arbitrates, exactly one row wins."""
    results = []

    def submit():
        r = client.post(
            "/jobs", json={"task_name": "echo", "idempotency_key": "race"}
        )
        results.append(r.json()["id"])

    threads = [threading.Thread(target=submit) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(set(results)) == 1
    count = conn.execute(
        "SELECT count(*) FROM jobs WHERE idempotency_key = 'race'"
    ).fetchone()[0]
    assert count == 1
