import os

# Point every module at the test database before anything imports config.
os.environ["DATABASE_URL"] = os.environ.get(
    "TEST_DATABASE_URL", "postgresql://localhost:5432/taskqueue_test"
)

import psycopg
import pytest

from taskqueue import db
from taskqueue.config import DATABASE_URL


@pytest.fixture(scope="session", autouse=True)
def migrated():
    """Rebuild the test database schema once per test session."""
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    db.migrate(DATABASE_URL)
    yield
    db.close_pool()


@pytest.fixture()
def conn():
    """Fresh connection with empty tables for each test."""
    with psycopg.connect(DATABASE_URL, autocommit=True) as c:
        c.execute("TRUNCATE jobs, workers RESTART IDENTITY")
        yield c
