import pytest
from fastapi.testclient import TestClient

from taskqueue.coordinator.app import app


@pytest.fixture()
def client(conn):
    with TestClient(app) as c:
        yield c


def test_submit_and_get_job(client):
    resp = client.post(
        "/jobs",
        json={"task_name": "echo", "payload": {"msg": "hi"}, "priority": 3},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["state"] == "queued"
    assert body["priority"] == 3
    assert body["payload"] == {"msg": "hi"}

    resp = client.get(f"/jobs/{body['id']}")
    assert resp.status_code == 200
    assert resp.json()["task_name"] == "echo"


def test_get_missing_job_404(client):
    assert client.get("/jobs/999999").status_code == 404


def test_submit_validates_input(client):
    assert client.post("/jobs", json={}).status_code == 422
    assert (
        client.post("/jobs", json={"task_name": "x", "max_attempts": 0}).status_code
        == 422
    )


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}
