CREATE TABLE IF NOT EXISTS dev.public.s3_load_tracker (
    partition_path        VARCHAR(1024),
    is_loaded             VARCHAR(20),
    status     VARCHAR(50),
    loaded_at  TIMESTAMP
);