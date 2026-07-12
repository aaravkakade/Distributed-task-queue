-- Job state machine:
--
--   queued ──claim──▶ running ──▶ succeeded          (terminal)
--     ▲                  │
--     │                  ├──▶ failed ──retry──▶ queued   (attempts < max_attempts)
--     │                  │
--     └──lease expired───┘   failed ──exhausted─▶ dead   (terminal, dead-letter)
--
-- All transitions happen in single SQL statements guarded by a WHERE clause on
-- the current state, so a stale actor (e.g. a worker whose lease was reaped)
-- can never clobber a newer transition.

CREATE TYPE job_state AS ENUM ('queued', 'running', 'succeeded', 'failed', 'dead');

CREATE TABLE jobs (
    id               BIGSERIAL PRIMARY KEY,
    idempotency_key  TEXT UNIQUE,
    task_name        TEXT        NOT NULL,
    payload          JSONB       NOT NULL DEFAULT '{}',
    state            job_state   NOT NULL DEFAULT 'queued',
    priority         INT         NOT NULL DEFAULT 0,

    attempts         INT         NOT NULL DEFAULT 0,
    max_attempts     INT         NOT NULL DEFAULT 5,

    -- earliest time the job may be claimed; pushed forward on retry (backoff)
    run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- set while running; the reaper re-queues jobs whose lease expired
    claimed_by       TEXT,
    lease_expires_at TIMESTAMPTZ,

    result           JSONB,
    last_error       TEXT,

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Claim path: workers scan only claimable jobs, in priority then FIFO order.
-- Partial index keeps it small no matter how much terminal history accumulates.
CREATE INDEX idx_jobs_claim ON jobs (priority DESC, run_at, id) WHERE state = 'queued';

-- Reaper path: find running jobs with expired leases.
CREATE INDEX idx_jobs_lease ON jobs (lease_expires_at) WHERE state = 'running';

CREATE TABLE workers (
    id             TEXT PRIMARY KEY,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now()
);
