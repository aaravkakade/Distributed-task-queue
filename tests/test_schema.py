import psycopg
import pytest


def test_job_defaults(conn):
    row = conn.execute(
        "INSERT INTO jobs (task_name) VALUES ('echo') RETURNING *"
    ).fetchone()
    assert row[4] == "queued"  # state column


def test_invalid_state_rejected(conn):
    with pytest.raises(psycopg.errors.InvalidTextRepresentation):
        conn.execute(
            "INSERT INTO jobs (task_name, state) VALUES ('echo', 'bogus')"
        )


def test_idempotency_key_unique(conn):
    conn.execute(
        "INSERT INTO jobs (task_name, idempotency_key) VALUES ('echo', 'k1')"
    )
    with pytest.raises(psycopg.errors.UniqueViolation):
        conn.execute(
            "INSERT INTO jobs (task_name, idempotency_key) VALUES ('echo', 'k1')"
        )


def test_migrate_is_idempotent():
    from taskqueue import db
    from taskqueue.config import DATABASE_URL

    assert db.migrate(DATABASE_URL) == []  # session fixture already applied it
