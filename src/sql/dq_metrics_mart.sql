-- Derived mart sketch (adjust to your warehouse); assumes dq_* base tables exist.
-- Example view: latest pass rate by dataset
CREATE OR REPLACE VIEW dq_v_dataset_pass_rate AS
SELECT dataset,
       AVG(CASE WHEN pass_flag THEN 1.0 ELSE 0.0 END) AS pass_rate,
       COUNT(*) AS checks_evaluated
FROM dq_rule_result r
JOIN dq_scan_run s USING (run_id)
GROUP BY dataset;
