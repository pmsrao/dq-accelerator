# Getting Started with Data Quality Accelerator

## 🚀 Quick Start

The Data Quality Accelerator is a Databricks-optimized framework for defining, executing, and monitoring data quality rules. This guide will help you get up and running quickly.

## 📋 Prerequisites

### Databricks Environment
- **Databricks Workspace**: Access to a Databricks workspace
- **Cluster**: Databricks cluster with appropriate permissions
- **Unity Catalog**: Access to Unity Catalog (recommended) or legacy Hive metastore

### Runtime Dependencies
The following packages are automatically available in Databricks runtime:
- **Spark**: Apache Spark for data processing
- **Delta Lake**: For ACID transactions and time travel
- **Soda Core**: For data quality checks (if using Soda engine)

## 🏗️ Installation

### 1. Clone the Repository
```bash
git clone https://github.com/your-org/dq-accelerator.git
cd dq-accelerator
```

### 2. Install Dependencies
```bash
# Install development dependencies
make install-dev

# Or install production dependencies
make install
```

### 3. Set Up Databricks Configuration
```bash
# Set your Databricks token
export DATABRICKS_TOKEN="your-databricks-token"

# Set your workspace URL
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
```

## 📝 Creating Your First DQ Rules

### 1. Define DQ Rules
Create a YAML file with your data quality rules:

```yaml
# payments_rules.yaml
dataset: "silver.payments"
description: "Data quality rules for payments dataset"

rules:
  - id: "payments.amount.non_negative.v01"
    name: "Amount must be non-negative"
    description: "Payment amounts should not be negative"
    engine: "soda"
    category: "validity"
    check_type: "numeric"
    threshold: 0
    sql: "SELECT COUNT(*) FROM {dataset} WHERE amount < 0"
    
  - id: "payments.pk.unique.v01"
    name: "Payment ID uniqueness"
    description: "Payment IDs must be unique"
    engine: "soda"
    category: "uniqueness"
    check_type: "unique"
    columns: ["payment_id"]
    
  - id: "payments.account_id.not_null.v01"
    name: "Account ID not null"
    description: "Account ID must not be null"
    engine: "sql"
    category: "completeness"
    check_type: "not_null"
    columns: ["account_id"]
```

### 2. Run DQ Rules

#### File-Based Rule Loading
```python
from src.libraries.dq_runner.databricks_runner import DQRunner

# Initialize the DQ Runner
dq_runner = DQRunner()

# Run incremental processing with single file
summary = dq_runner.run_incremental(
    rule_path="payments_rules.yaml",  # Single file
    dataset="silver.payments",
    watermark_column="event_date",
    environment="dev"  # Environment-aware catalog resolution
)

print(f"Results: {summary.passed_rules}/{summary.total_rules} rules passed")
```

#### Folder-Based Rule Loading
```python
# Run with multiple rule files from a folder
summary = dq_runner.run_incremental(
    rule_path="rules/domain_rules/",  # Folder containing multiple YAML files
    dataset="silver.payments",
    watermark_column="event_date",
    environment="prod"
)

print(f"Results: {summary.passed_rules}/{summary.total_rules} rules passed")
print(f"Source files: {summary.metadata.get('source_files', [])}")
```

## 🌍 Environment-Aware Configuration

The DQ Accelerator supports environment-aware table resolution and configuration:

### 1. Environment-Specific Catalogs
```python
# Development environment
summary = dq_runner.run_incremental(
    rule_path="rules/payments.yaml",
    dataset="silver.payments",  # Resolves to dev_catalog.silver.payments
    watermark_column="event_date",
    environment="dev"
)

# Production environment  
summary = dq_runner.run_incremental(
    rule_path="rules/payments.yaml",
    dataset="silver.payments",  # Resolves to main.silver.payments
    watermark_column="event_date", 
    environment="prod"
)
```

### 2. SQL Query Resolution
```sql
-- Original SQL in rule file
FROM silver.payments p
LEFT JOIN silver.accounts a ON p.account_id = a.account_id

-- Automatically resolved for prod environment
FROM main.silver.payments p
LEFT JOIN main.silver.accounts a ON p.account_id = a.account_id
```

### 3. Rule Aggregation
```python
# Load rules from multiple files in a folder
summary = dq_runner.run_incremental(
    rule_path="rules/domain_rules/",  # Aggregates all YAML files
    dataset="silver.payments",
    watermark_column="event_date",
    environment="prod"
)

# Access metadata about loaded rules
print(f"Total rules: {summary.metadata['total_rules']}")
print(f"Source files: {summary.metadata['source_files']}")
```

## 🔄 Incremental Processing

The DQ Accelerator supports incremental processing using watermarks:

### 1. Set Up Watermark Table
```sql
-- Create watermark table (automatically created by WatermarkManager)
CREATE TABLE IF NOT EXISTS dq_watermarks (
    dataset STRING,
    watermark_column STRING,
    watermark_value STRING,
    dq_run_completed_ts TIMESTAMP,
    updated_ts TIMESTAMP
) USING DELTA
PARTITIONED BY (dataset);
```

### 2. Run Incremental Processing
```python
# First run - processes all data
summary1 = dq_runner.run_incremental(
    rule_path="payments_rules.yaml",  # File or folder
    dataset="silver.payments",
    watermark_column="event_date",
    environment="dev"
)

# Subsequent runs - only process new data
summary2 = dq_runner.run_incremental(
    rule_path="payments_rules.yaml",
    dataset="silver.payments", 
    watermark_column="event_date",
    environment="dev"
)
```

## 🚀 Databricks Workflow Integration

### 1. Create Databricks Jobs
```python
from src.libraries.integrations.databricks_workflow import DatabricksWorkflowManager

# Initialize workflow manager
workflow_manager = DatabricksWorkflowManager()

# Create a DQ job
job_id = workflow_manager.create_dq_job(
    job_name="daily_payments_dq",
    rules_path="payments_rules.yaml",  # File or folder
    dataset="silver.payments",
    schedule="0 2 * * *",  # Daily at 2 AM
    incremental=True,
    watermark_column="event_date",
    environment="prod"  # Environment-aware configuration
)
```

### 2. Deploy Workflows
```bash
# Deploy all workflows
python scripts/deploy_databricks_workflows.py deploy \
    --config config/databricks_workflow_config.yaml \
    --environment prod
```

## 📊 Monitoring and Metrics

### 1. View DQ Results
```sql
-- Query DQ results
SELECT 
    rule_id,
    pass_flag,
    execution_time_ms,
    error_message
FROM dq_results
WHERE dataset = 'silver.payments'
ORDER BY execution_ts DESC;
```

### 2. Generate Health Summary
```python
from src.libraries.utils.metrics_mart_populator import MetricsMartPopulator

# Initialize metrics populator
metrics_populator = MetricsMartPopulator()

# Get dataset health summary
health = metrics_populator.get_dataset_health_summary("silver.payments")
print(f"Health Score: {health['health_score']}/100")
print(f"Health Status: {health['health_status']}")
```

## 🛠️ Development Workflow

### 1. Local Development
```bash
# Run tests
make test-unit

# Format code
make format

# Lint code
make lint

# Run example
make run-example
```

### 2. Rule Validation
```bash
# Validate rules against schema
make validate-rules
```

### 3. Build Package
```bash
# Build wheel package
make build
```

## 📚 Examples

### Complete Workflow Example
```python
# See src/schemas/examples/complete_workflow_example.py
# Demonstrates end-to-end workflow including:
# - Rule definition
# - Incremental processing
# - Metrics generation
# - Health monitoring
```

### Databricks Simple Example
```python
# See src/schemas/examples/databricks_simple_example.py
# Simplified example for Databricks environments
```

## 🔧 Configuration

### Environment Configuration
```yaml
# config/prod.yaml
catalog: main
default_schema: silver
table_prefix: ""

# Data quality thresholds
dq_thresholds:
  pass_rate_warning: 99.0
  pass_rate_critical: 98.0
  execution_time_warning_ms: 60000
  execution_time_critical_ms: 120000

# Notification settings
notifications:
  enabled: true
  channels: ["slack", "email", "pagerduty"]
  slack_channel: "#data-quality-alerts"
  email_recipients: ["data-team@company.com"]
```

### Development Environment
```yaml
# config/dev.yaml
catalog: dev_catalog
default_schema: silver
table_prefix: dev_

# More lenient thresholds for development
dq_thresholds:
  pass_rate_warning: 95.0
  pass_rate_critical: 90.0
  execution_time_warning_ms: 30000
  execution_time_critical_ms: 60000

# Minimal notifications for dev
notifications:
  enabled: false
  channels: []
```

### Workflow Configuration
```yaml
# config/databricks_workflow_config.yaml
workflows:
  daily_dq_processing:
    schedule: "0 2 * * *"
    incremental: true
    environment: "prod"  # Environment-aware configuration
    datasets:
      - "silver.payments"
      - "silver.accounts"
    
    dataset_configs:
      "silver.payments":
        watermark_column: "event_date"
        rules_path: "src/schemas/examples/payments_rules.yaml"  # File or folder
        environment: "prod"
```

### Rule Organization Examples

#### File-Based Organization
```
rules/
├── payments_rules.yaml
├── customers_rules.yaml
└── transactions_rules.yaml
```

#### Folder-Based Organization
```
rules/
├── domain_rules/
│   ├── payments/
│   │   ├── basic_checks.yaml
│   │   └── advanced_checks.yaml
│   └── customers/
│       └── customer_checks.yaml
└── cross_domain/
    └── referential_integrity.yaml
```

## 🚨 Troubleshooting

### Common Issues

**1. Spark Session Not Available**
```python
# Ensure you're running in Databricks environment
# The DQ Runner assumes 'spark' is available globally
```

**2. Soda Core Import Error**
```python
# Ensure you're using Databricks runtime with Soda Core
# Or install manually: pip install soda-core-spark-df
```

**3. Watermark Table Not Found**
```python
# WatermarkManager automatically creates the table
# Ensure you have CREATE TABLE permissions
```

### Debug Mode
```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Run with debug logging
dq_runner = DQRunner()
```

## 📖 Next Steps

1. **Explore Examples**: Check out the examples in `src/schemas/examples/`
2. **Read Architecture**: Review `docs/architecture.md` for detailed system design
3. **Customize Rules**: Create your own DQ rules following the schema
4. **Set Up Monitoring**: Configure metrics and alerting
5. **Scale Up**: Deploy to production with proper configuration

## 🆘 Support

- **Documentation**: Check `docs/` directory for detailed guides
- **Examples**: See `src/schemas/examples/` for usage examples
- **Issues**: Report issues on GitHub
- **Architecture**: Review `docs/architecture.md` for system design

## 🎯 Best Practices

1. **Use Incremental Processing**: Always use watermarks for large datasets
2. **Validate Rules**: Use `make validate-rules` before deployment
3. **Monitor Health**: Set up regular health checks and alerting
4. **Version Control**: Keep rule files in version control
5. **Test Locally**: Use `make test-unit` before deploying changes

Happy data quality checking! 🎉
