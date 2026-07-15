\c warehouse;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS metadata;
CREATE SCHEMA IF NOT EXISTS audit;

-- =========================
-- WATERMARK
-- =========================
CREATE TABLE IF NOT EXISTS metadata.watermark (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    last_processed_id BIGINT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- =========================
-- DAG AUDIT
-- =========================
CREATE TABLE IF NOT EXISTS metadata.dag_run_audit (
    dag_id VARCHAR(255),
    run_id VARCHAR(255),
    dag_version VARCHAR(50),
    git_sha VARCHAR(100),
    execution_date TIMESTAMP,
    start_time TIMESTAMP DEFAULT NOW(),
    end_time TIMESTAMP,
    status VARCHAR(50)
);

-- =========================
-- TASK METRICS
-- =========================
CREATE TABLE IF NOT EXISTS metadata.task_metrics (
    dag_id VARCHAR(255),
    task_id VARCHAR(255),
    run_id VARCHAR(255),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds NUMERIC(18,2),
    status VARCHAR(50)
);

-- =========================
-- DATA FRESHNESS
-- =========================
CREATE TABLE IF NOT EXISTS metadata.data_freshness (
    dataset_name VARCHAR(255) PRIMARY KEY,
    last_watermark BIGINT,
    updated_at TIMESTAMP
);

-- =========================
-- AUDIT LOG
-- =========================
CREATE TABLE IF NOT EXISTS audit.etl_run_log (
    id BIGSERIAL PRIMARY KEY,
    dag_id VARCHAR(255),
    task_id VARCHAR(255),
    status VARCHAR(50),
    rows_processed BIGINT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    error_message TEXT
);

-- =========================
-- SALES BRONZE / SILVER / GOLD
-- =========================
CREATE TABLE IF NOT EXISTS bronze.sales (
    id BIGINT PRIMARY KEY,
    product VARCHAR(255),
    amount NUMERIC(18,2),
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.sales (
    id BIGINT PRIMARY KEY,
    product VARCHAR(255),
    amount NUMERIC(18,2),
    amount_with_tax NUMERIC(18,2),
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.sales_summary (
    product VARCHAR(255) PRIMARY KEY,
    total_sales NUMERIC(18,2),
    total_orders BIGINT
);