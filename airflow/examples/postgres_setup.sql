-- CREATE TABLE sales_raw (
--     id SERIAL PRIMARY KEY,
--     product TEXT,
--     amount NUMERIC,
--     created_at TIMESTAMP DEFAULT NOW()
-- );

-- INSERT INTO sales_raw (product, amount) VALUES
-- ('laptop', 1000),
-- ('mouse', 50),
-- ('keyboard', 80);


-- CREATE SCHEMA IF NOT EXISTS raw;
-- CREATE SCHEMA IF NOT EXISTS staging;
-- CREATE SCHEMA IF NOT EXISTS warehouse;


-- CREATE TABLE IF NOT EXISTS raw.sales_raw (
--     id SERIAL PRIMARY KEY,
--     product TEXT,
--     amount NUMERIC,
--     created_at TIMESTAMP DEFAULT NOW()
-- );
-- CREATE TABLE IF NOT EXISTS staging.sales_clean (
--     id INT,
--     product TEXT,
--     amount NUMERIC,
--     amount_with_tax NUMERIC,
--     created_at TIMESTAMP
-- );
-- CREATE TABLE IF NOT EXISTS warehouse.sales_summary (
--     product TEXT,
--     total_sales NUMERIC,
--     total_orders INT
-- );
-- INSERT INTO raw.sales_raw (product, amount) VALUES
-- ('laptop', 1000),
-- ('mouse', 50),
-- ('keyboard', 80),
-- ('mouse', 60);

-- CREATE TABLE IF NOT EXISTS raw.etl_control (
--     job_name TEXT PRIMARY KEY,
--     last_processed_id INT DEFAULT 0
-- );
-- CREATE TABLE IF NOT EXISTS raw.etl_log (
--     id SERIAL PRIMARY KEY,
--     job_name TEXT,
--     status TEXT,
--     rows_processed INT,
--     started_at TIMESTAMP DEFAULT NOW(),
--     finished_at TIMESTAMP
-- );

-- CREATE SCHEMA audit;
-- CREATE SCHEMA metadata;
-- CREATE TABLE audit.etl_run_log (
--     run_id BIGSERIAL PRIMARY KEY,
--     dag_id TEXT,
--     task_id TEXT,
--     status TEXT,
--     rows_processed BIGINT,
--     start_time TIMESTAMP,
--     end_time TIMESTAMP,
--     error_message TEXT
-- );	

-- CREATE TABLE metadata.watermark (
--     pipeline_name TEXT PRIMARY KEY,
--     last_processed_id BIGINT,
--     last_processed_date TIMESTAMP
-- );
-- ALTER TABLE metadata.watermark
-- ADD COLUMN updated_at TIMESTAMP;
-- -- ----------- OR USE THIS BELOW
-- CREATE TABLE IF NOT EXISTS metadata.watermark (
--     pipeline_name TEXT PRIMARY KEY,
--     last_processed_id BIGINT,
--     updated_at TIMESTAMP DEFAULT NOW()
-- );

-- UPDATE metadata.watermark
-- SET updated_at = NOW()
-- WHERE updated_at IS NULL;