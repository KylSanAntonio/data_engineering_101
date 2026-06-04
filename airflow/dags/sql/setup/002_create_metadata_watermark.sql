CREATE TABLE IF NOT EXISTS metadata.watermark
(
    pipeline_name TEXT PRIMARY KEY,
    last_processed_id BIGINT,
    updated_at TIMESTAMP DEFAULT NOW()
);