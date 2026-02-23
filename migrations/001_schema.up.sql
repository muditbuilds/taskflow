-- Tasks: single units of work (standalone or DAG steps)
CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    priority INT NOT NULL DEFAULT 0,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    run_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    dag_run_id UUID,
    dag_step_id VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_tasks_status_scheduled ON tasks (status, scheduled_at) WHERE status IN ('pending', 'queued');
CREATE INDEX idx_tasks_dag_run ON tasks (dag_run_id) WHERE dag_run_id IS NOT NULL;
CREATE INDEX idx_tasks_dag_step ON tasks (dag_run_id, dag_step_id) WHERE dag_run_id IS NOT NULL;

-- DAG runs: one row per DAG execution
CREATE TABLE IF NOT EXISTS dag_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    definition JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dag_runs_status ON dag_runs (status) WHERE status IN ('pending', 'running');

-- Dead-letter: failed tasks that exceeded retries (we use status=dlq and same table; optional separate table for clarity)
-- We keep DLQ as status on tasks; no separate table for simplicity.
-- Optional: table for DLQ replay metadata if needed later.
