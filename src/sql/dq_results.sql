-- DQ Results Store (generic SQL; adapt to Databricks/Delta)
CREATE TABLE IF NOT EXISTS dq_scan_run (
  run_id VARCHAR(64) PRIMARY KEY,
  dataset VARCHAR(512),
  partition_value VARCHAR(128),
  product VARCHAR(256), domain VARCHAR(256),
  trigger_type VARCHAR(64),
  engine_versions TEXT,
  started_ts TIMESTAMP, ended_ts TIMESTAMP,
  status VARCHAR(32),
  hwm_value VARCHAR(128),
  orchestrator VARCHAR(128),
  extras TEXT
);

CREATE TABLE IF NOT EXISTS dq_rule_result (
  run_id VARCHAR(64),
  rule_id VARCHAR(512),
  dq_category VARCHAR(64),
  dq_check_type VARCHAR(64),
  engine VARCHAR(16),
  severity VARCHAR(16),
  pass_flag BOOLEAN,
  pass_count BIGINT,
  fail_count BIGINT,
  measured_value TEXT,
  threshold TEXT,
  exec_ms BIGINT,
  error_message TEXT,
  sample_ref VARCHAR(64),
  created_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dq_rule_measure (
  run_id VARCHAR(64),
  rule_id VARCHAR(512),
  metric_name VARCHAR(128),
  metric_value TEXT
);

CREATE TABLE IF NOT EXISTS dq_failure_sample (
  sample_ref VARCHAR(64) PRIMARY KEY,
  run_id VARCHAR(64),
  rule_id VARCHAR(512),
  sample TEXT,
  ttl_expiry_ts TIMESTAMP
);

-- Watermark table moved to dq_watermarks.sql for better organization
