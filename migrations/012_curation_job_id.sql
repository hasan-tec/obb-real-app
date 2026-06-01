-- Migration 012: add job_id to curation_runs so status survives dyno restarts
-- The in-memory _jobs dict is wiped on Heroku redeploy/dyno cycling.
-- Storing job_id in DB lets the status endpoint fall back to curation_runs.status.

ALTER TABLE curation_runs ADD COLUMN IF NOT EXISTS job_id TEXT;
CREATE INDEX IF NOT EXISTS idx_curation_runs_job_id ON curation_runs (job_id);
