"""Coordinator: HTTP API for submitting jobs and querying their status."""

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
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
def submit_job(job: JobSubmit) -> JobOut:
    with db.get_pool().connection() as conn:
        row = conn.execute(
            """INSERT INTO jobs (task_name, payload, priority, max_attempts)
               VALUES (%s, %s, %s, %s)
               RETURNING *""",
            (job.task_name, Jsonb(job.payload), job.priority, job.max_attempts),
        ).fetchone()
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
