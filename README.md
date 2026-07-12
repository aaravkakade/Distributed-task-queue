# Distributed Task Queue

A distributed, fault-tolerant task queue (a mini Celery/SQS) built from scratch on
PostgreSQL — **no separate message broker**. Postgres is both the durable job store
and the coordination substrate: row locks arbitrate job claims, leases + a reaper
detect dead workers, a UNIQUE index enforces idempotency, and advisory locks
elect a leader.

**Stack:** Python · FastAPI (coordinator) · PostgreSQL · plain worker processes ·
Docker Compose.

## Architecture

```
                        ┌──────────────┐
   clients ──HTTP──▶    │ coordinator  │   POST /jobs   (idempotency keys)
                        │  (FastAPI)   │   GET  /jobs/{id}, /jobs?state=…
                        └──────┬───────┘   POST /jobs/{id}/retry   GET /stats
                               │ INSERT
                               ▼
                     ┌───────────────────┐
                     │    PostgreSQL     │   jobs table = queue + state machine
                     │                   │   workers table = liveness registry
                     │  FOR UPDATE       │   advisory lock = leader election
                     │  SKIP LOCKED      │
                     └──┬─────┬─────┬────┘
              claim ▲   │     │     │   ▲ heartbeat: extend lease
                    │   ▼     ▼     ▼   │
                 ┌────────┐ ┌────────┐ ┌────────┐      ┌─────────────────┐
                 │worker 1│ │worker 2│ │worker N│      │ reaper (leader) │
                 └────────┘ └────────┘ └────────┘      │ reaper (standby)│
                    each claims jobs concurrently;     └────────┬────────┘
                    no two ever get the same job          re-queues jobs with
                                                          expired leases
```

Every component is a separate process (a separate container under Compose); they
communicate only through the database.

## Job state machine

```
  queued ──claim──▶ running ──▶ succeeded                     (terminal)
    ▲                  │
    │                  ├──▶ failed ──(backoff elapses)──▶ claimed again
    │                  │        │
    └──lease expired───┘        └──attempts exhausted──▶ dead  (terminal, DLQ)
        (reaper)
```

| State | Meaning |
|---|---|
| `queued` | Waiting to be claimed. `run_at` gates the earliest claim time. |
| `running` | Claimed by a worker holding a lease (`lease_expires_at`). |
| `failed` | An attempt failed; waiting out exponential backoff, then claimable again. |
| `succeeded` | Terminal. `result` holds the task's return value. |
| `dead` | Terminal. Retries exhausted — the dead-letter queue. Revive via API. |

Every transition is a single guarded SQL statement
(`UPDATE … WHERE state = <expected> AND claimed_by = <me>`), so a stale actor —
say a worker whose lease was already reaped — matches zero rows instead of
clobbering a newer transition.

## Design decisions

**Claiming without a broker.** Workers race on
`UPDATE … WHERE id = (SELECT … FOR UPDATE SKIP LOCKED LIMIT 1)`. Row locking
guarantees exactly one claimant per job; `SKIP LOCKED` makes losers skip to the
next row instead of blocking. A partial index on claimable states keeps the scan
fast regardless of how much terminal history accumulates.

**Failure detection = leases, not liveness checks.** A claim stamps
`lease_expires_at`; a heartbeat thread extends it while the task runs. A worker
that dies (`kill -9`, OOM, partition) simply stops renewing. The reaper re-queues
any running job whose lease lapsed — no coordinator ever needs to reach a worker.

**At-least-once delivery.** A reaped job may have been partially executed, so
re-queueing it means a task can run twice. That's the deliberate trade-off
(exactly-once requires coordination that mostly doesn't survive contact with
reality); task handlers are expected to be idempotent.

**Idempotent enqueue.** `POST /jobs` with an `idempotency_key` does
`INSERT … ON CONFLICT DO NOTHING`; the UNIQUE index arbitrates, so client
retries — even racing against multiple coordinators — create exactly one job.
Replays return the original job with `200` instead of `201`.

**Retries with backoff, then dead-letter.** A failed attempt atomically becomes
`failed` with `run_at = now() + min(cap, base·2^(attempt−1))`, or `dead` once
attempts are exhausted (this also stops poison pills that kill workers from
cycling forever). Dead jobs are inspectable (`GET /jobs?state=dead`) and
revivable (`POST /jobs/{id}/retry`).

**Leader election.** Reapers run replicated; whoever holds
`pg_try_advisory_lock` is leader. Postgres releases the lock the moment the
holder's session dies, so failover requires no timeout bookkeeping.

## Measured performance

No-op (`echo`) jobs, so the numbers measure queue overhead — claim contention,
dispatch, success write — not task work. MacBook Air M2, PostgreSQL 18, all
processes local, wall-clock from worker launch to last job succeeded
(`scripts/benchmark.py`):

| Setup | Throughput |
|---|---|
| 10,000 jobs, 1 worker | ~3,100 jobs/sec |
| 10,000 jobs, 4 workers | ~8,500 jobs/sec |
| 10,000 jobs, 8 workers | ~11,400 jobs/sec |

**Recovery after `kill -9`:** 1.0s with a 1-second lease (test
`test_kill9_recovery_end_to_end` SIGKILLs a real worker subprocess mid-job and
times until the job is claimable again). In general, recovery ≤ remaining lease
+ one reap interval — a tunable durability/latency trade-off.

**Correctness under contention:** 8 workers racing over 300 jobs produce zero
double-claims and every job runs exactly once (`test_no_double_claim_under_load`).

## Running it

### Docker Compose (coordinator + 3 workers + 2 reapers + Postgres)

```sh
docker compose up --build
docker compose up --scale worker=8          # more workers
docker compose kill -s SIGKILL worker       # failure drill: watch the reaper

curl -X POST localhost:8000/jobs -H 'content-type: application/json' \
     -d '{"task_name": "sleep", "payload": {"seconds": 5}, "idempotency_key": "demo-1"}'
curl localhost:8000/stats
```

### Local development

```sh
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
createdb taskqueue && createdb taskqueue_test
.venv/bin/python -m taskqueue.db                                # migrate
.venv/bin/uvicorn taskqueue.coordinator.app:app                 # coordinator
.venv/bin/python -m taskqueue.worker.worker                     # worker (run several)
.venv/bin/python -m taskqueue.reaper                            # reaper
.venv/bin/pytest                                                # 36 tests
.venv/bin/python scripts/benchmark.py 10000 8                   # throughput
```

## API

| Endpoint | Purpose |
|---|---|
| `POST /jobs` | Enqueue (task_name, payload, priority, max_attempts, idempotency_key) |
| `GET /jobs/{id}` | Job status, result, error, attempt count |
| `GET /jobs?state=dead` | List jobs by state (the DLQ view) |
| `POST /jobs/{id}/retry` | Revive a dead job with a fresh attempt budget |
| `GET /stats` | Queue depth per state, backlog age, live workers, throughput |
| `GET /healthz` | Liveness (checks DB round-trip) |

## Layout

```
taskqueue/
  migrations/        schema + state machine (SQL, applied in order)
  db.py              connection pool, migration runner
  config.py          tunables (lease, heartbeat, backoff, intervals) via env
  coordinator/app.py FastAPI: submit, query, DLQ, stats
  worker/worker.py   claim loop, heartbeat thread, retry/DLQ transitions
  reaper.py          lease-expiry recovery, leader-elected
  leader.py          advisory-lock leader election
  tasks.py           task registry (+ failure-injection tasks for tests)
tests/               36 tests: schema, API, concurrency, leases, reaper,
                     kill -9 drill, idempotency, retries, election, stats
scripts/benchmark.py measured throughput
```
