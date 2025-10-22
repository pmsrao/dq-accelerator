-- DQ Watermarks Store (Delta Lake)
-- Tracks high water marks for incremental data quality processing
-- In Unity Catalog, table location is managed by the catalog and schema

CREATE TABLE IF NOT EXISTS dq_watermarks (
  dataset STRING NOT NULL COMMENT 'Dataset name (e.g., silver.payments)',
  watermark_column STRING NOT NULL COMMENT 'Column used for incremental processing (e.g., event_date, batch_id)',
  watermark_value STRING NOT NULL COMMENT 'Last successfully processed value of the watermark column',
  dq_run_completed_ts TIMESTAMP NOT NULL COMMENT 'Timestamp when the DQ run completed successfully',
  updated_ts TIMESTAMP NOT NULL COMMENT 'Timestamp when this watermark record was last updated'
) USING DELTA
COMMENT 'High water marks for incremental data quality processing';

-- Create index on dataset for efficient lookups
CREATE INDEX IF NOT EXISTS idx_dq_watermarks_dataset 
ON dq_watermarks (dataset);

-- Example usage:
-- INSERT INTO dq_watermarks VALUES 
--   ('silver.payments', 'event_date', '2025-01-15', '2025-01-15 10:30:00', '2025-01-15 10:30:00');

-- Query to get watermark for a dataset:
-- SELECT * FROM dq_watermarks WHERE dataset = 'silver.payments';

-- Query to get all watermarks:
-- SELECT * FROM dq_watermarks ORDER BY updated_ts DESC;
