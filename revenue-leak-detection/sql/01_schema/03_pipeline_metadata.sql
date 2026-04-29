-- =============================================================
-- 03_pipeline_metadata.sql
-- Tables that the pipeline writes to itself: run history + DQ results.
-- Powers the "pipeline monitoring" dashboard later.
-- =============================================================

USE portfolio;

DROP TABLE IF EXISTS pipeline_run_log;
CREATE TABLE pipeline_run_log (
    run_id          BIGINT       PRIMARY KEY AUTO_INCREMENT,
    asset_name      VARCHAR(100) NOT NULL,
    partition_key   VARCHAR(40),                  -- e.g., '2024-01-15' for daily partitions
    started_at      DATETIME     NOT NULL,
    finished_at     DATETIME,
    status          ENUM('running','success','failed','skipped') NOT NULL,
    rows_written    BIGINT,
    error_message   TEXT,
    KEY idx_asset_dt (asset_name, started_at),
    KEY idx_partition (asset_name, partition_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS pipeline_dq_results;
CREATE TABLE pipeline_dq_results (
    dq_id        BIGINT       PRIMARY KEY AUTO_INCREMENT,
    run_id       BIGINT,
    asset_name   VARCHAR(100) NOT NULL,
    check_name   VARCHAR(100) NOT NULL,
    severity     ENUM('info','warn','error') NOT NULL DEFAULT 'warn',
    passed       BOOLEAN      NOT NULL,
    metric_value DECIMAL(18,4),
    threshold    DECIMAL(18,4),
    checked_at   DATETIME     NOT NULL,
    notes        VARCHAR(500),
    KEY idx_asset (asset_name, checked_at),
    KEY idx_run (run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
