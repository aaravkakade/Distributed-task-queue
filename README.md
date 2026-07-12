# Distributed Task Queue

A distributed, fault-tolerant task queue (a mini Celery/SQS) built from scratch on
PostgreSQL — no separate message broker.

**Stack:** Python, FastAPI (coordinator), PostgreSQL (durable job store), plain
Python worker processes, Docker Compose.

## Core design

- A coordinator accepts jobs over HTTP and writes them to a Postgres `jobs` table.
- Workers claim jobs concurrently with `SELECT ... FOR UPDATE SKIP LOCKED` —
  safe under concurrency, no broker needed.
- Workers hold time-bound **leases** renewed by heartbeats; a **reaper** detects
  dead workers via expired leases and re-queues their in-flight jobs
  (**at-least-once delivery**).
- **Idempotency keys** prevent double-enqueue; retries use **exponential
  backoff**; jobs that exhaust retries land in a **dead-letter** state.

## Job state machine

```
  queued ──claim──▶ running ──▶ succeeded            (terminal)
    ▲                  │
    │                  ├──▶ failed ──retry──▶ queued     (attempts < max_attempts)
    │                  │
    └──lease expired───┘    failed ──exhausted──▶ dead   (terminal, dead-letter)
```

Every transition is a single SQL `UPDATE ... WHERE state = <expected>` statement,
so a stale actor (e.g. a worker whose lease was already reaped) can never clobber
a newer transition — the guarded update simply matches zero rows.

| State | Meaning |
|---|---|
| `queued` | Waiting to be claimed. `run_at` gates earliest claim time (backoff). |
| `running` | Claimed by a worker; `lease_expires_at` bounds how long it may hold the job. |
| `succeeded` | Terminal. `result` holds the task's return value. |
| `failed` | Attempt failed; transient state — immediately re-queued or moved to `dead`. |
| `dead` | Terminal. Retries exhausted; dead-letter queue for inspection/requeue. |

## Development

```sh
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
createdb taskqueue && createdb taskqueue_test
.venv/bin/python -m taskqueue.db          # apply migrations
.venv/bin/pytest
```
