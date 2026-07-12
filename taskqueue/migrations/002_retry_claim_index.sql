-- 'failed' now means "attempt failed, waiting out its backoff" and is
-- claimable once run_at arrives, so the claim index must cover it too.
DROP INDEX idx_jobs_claim;
CREATE INDEX idx_jobs_claim ON jobs (priority DESC, run_at, id)
    WHERE state IN ('queued', 'failed');
