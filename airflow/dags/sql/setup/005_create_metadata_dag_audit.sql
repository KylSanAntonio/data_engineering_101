CREATE TABLE IF NOT EXISTS metadata.dag_run_audit (
    dag_id TEXT,
    run_id TEXT,
    dag_version TEXT,
    git_sha TEXT,
    execution_date TIMESTAMP,
    start_time TIMESTAMP DEFAULT NOW(),
    end_time TIMESTAMP,
    status TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);