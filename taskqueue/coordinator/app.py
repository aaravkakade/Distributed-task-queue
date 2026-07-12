"""Coordinator: HTTP API for submitting jobs and querying their status."""

from contextlib import asynccontextmanager
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Response
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field

from taskqueue import db


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.migrate()
    db.get_pool()
    yield
    db.close_pool()


app = FastAPI(title="taskqueue coordinator", lifespan=lifespan)


class JobSubmit(BaseModel):
    task_name: str = Field(min_length=1)
    payload: dict[str, Any] = {}
    priority: int = 0
    max_attempts: int = Field(default=5, ge=1)
    idempotency_key: str | None = Field(default=None, min_length=1)


class JobOut(BaseModel):
    id: int
    task_name: str
    state: str
    priority: int
    attempts: int
    max_attempts: int
    payload: dict[str, Any]
    result: Any = None
    last_error: str | None = None
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None


def _job_out(row: dict) -> JobOut:
    return JobOut(
        **{
            k: (v.isoformat() if k.endswith("_at") and v is not None else v)
            for k, v in row.items()
            if k in JobOut.model_fields
        }
    )


@app.post("/jobs", status_code=201)
def submit_job(job: JobSubmit, response: Response) -> JobOut:
    """Enqueue a job. With an idempotency_key, retried submissions (client
    timeout, network blip, double-click) return the original job instead of
    enqueueing a duplicate: ON CONFLICT DO NOTHING makes the race safe even
    across coordinators, since the UNIQUE index is the arbiter."""
    with db.get_pool().connection() as conn:
        row = conn.execute(
            """INSERT INTO jobs (task_name, payload, priority, max_attempts,
                                 idempotency_key)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (idempotency_key) DO NOTHING
               RETURNING *""",
            (job.task_name, Jsonb(job.payload), job.priority, job.max_attempts,
             job.idempotency_key),
        ).fetchone()
        if row is None:  # replay: key already used
            row = conn.execute(
                "SELECT * FROM jobs WHERE idempotency_key = %s",
                (job.idempotency_key,),
            ).fetchone()
            response.status_code = 200
    return _job_out(row)


@app.get("/jobs")
def list_jobs(
    state: Literal["queued", "running", "succeeded", "failed", "dead"] | None = None,
    limit: int = 100,
) -> list[JobOut]:
    with db.get_pool().connection() as conn:
        rows = conn.execute(
            """SELECT * FROM jobs
                WHERE %(state)s::job_state IS NULL OR state = %(state)s
                ORDER BY id DESC LIMIT %(limit)s""",
            {"state": state, "limit": min(limit, 1000)},
        ).fetchall()
    return [_job_out(r) for r in rows]


@app.post("/jobs/{job_id}/retry")
def retry_dead_job(job_id: int) -> JobOut:
    """Requeue a dead-lettered job with a fresh attempt budget."""
    with db.get_pool().connection() as conn:
        row = conn.execute(
            """UPDATE jobs
                  SET state = 'queued', attempts = 0, run_at = now(),
                      finished_at = NULL, updated_at = now()
                WHERE id = %s AND state = 'dead'
               RETURNING *""",
            (job_id,),
        ).fetchone()
        if row is None:
            exists = conn.execute(
                "SELECT 1 FROM jobs WHERE id = %s", (job_id,)
            ).fetchone()
            raise HTTPException(
                status_code=404 if exists is None else 409,
                detail="job not found" if exists is None else "job is not dead",
            )
    return _job_out(row)


@app.get("/jobs/{job_id}")
def get_job(job_id: int) -> JobOut:
    with db.get_pool().connection() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id = %s", (job_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="job not found")
    return _job_out(row)


@app.get("/healthz")
def healthz() -> dict:
    with db.get_pool().connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}
