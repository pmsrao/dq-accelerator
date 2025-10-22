# Data Quality Accelerator

A standardized framework for defining, executing, and monitoring data quality rules in Databricks environments.

## 🚀 Quick Start

### Installation

```bash
# Install development dependencies
make install-dev

# Or install production dependencies
make install
```

### Define Your First Rules

Create a YAML file with your data quality rules:

```yaml
# src/schemas/examples/my_rules.yaml
dataset: silver.payments
product: Payments
domain: Trading
owner: payments_dp@company.com

partitions:
  by: event_date
  format: yyyy-MM-dd

defaults:
  severity: high
  engine: soda
  run_policy: soft_fail

rules:
  - id: payments.amount.non_negative.v01
    dq_category: validity
    dq_check_type: value_range
    engine: soda
    columns: [amount]
    params:
      operator: ">="
      operand: 0
    sla:
      target_pass_rate: 99.5
      window_days: 7
```

### Execute DQ Rules

```bash
# Run example rules
make run-example

# Or manually with custom configuration
python -m src.dq_runner.runner \
  --rule-file src/schemas/examples/payments_rules.yaml \
  --config config/dev.yaml \
  --partition 2025-01-01 \
  --verbose
```

## 📋 Development

### Setup Development Environment

```bash
# Install dependencies and setup pre-commit hooks
make dev-setup

# Run quick development cycle
make quick-check
```

### Available Commands

```bash
# Code quality
make format          # Format code
make lint           # Run linting
make test           # Run tests

# Testing
make test-unit      # Unit tests only
make test-integration # Integration tests
make test-e2e       # End-to-end tests
make test-coverage  # Coverage report

# Examples
make run-example    # Run example DQ rules
make validate-rules # Validate example rules

# Build and deployment
make build          # Build package
make clean          # Clean artifacts
```

## 🔧 Configuration

### Environment Configuration

The project supports environment-specific configurations:

- `config/dev.yaml` - Development environment
- `config/staging.yaml` - Staging environment  
- `config/prod.yaml` - Production environment

### Example Configuration

```yaml
# config/dev.yaml
environment: dev

databricks:
  workspace_url: "https://dev-workspace.cloud.databricks.com"
  cluster_id: "dev-cluster-id"

dq:
  default_engine: "soda"
  default_severity: "medium"
  default_run_policy: "soft_fail"
  max_retries: 3
  retry_delay_seconds: 30

storage:
  results_table: "dev_dq_results"
  metrics_table: "dev_dq_metrics"
  watermark_table: "dev_dq_watermarks"
```

## 🔌 Integration

### Databricks Workflows

```python
from src.dq_runner import DQRunner

runner = DQRunner(config=config)
results, summary = runner.execute_rules(
    rules_spec=rules_spec,
    dataset="silver.payments",
    partition_value="2025-01-01"
)
```

### Airflow Integration

```python
from airflow.operators.python import PythonOperator
from src.dq_runner import DQRunner

def run_dq_checks(**context):
    runner = DQRunner()
    results, summary = runner.execute_rules(
        rules_spec=load_rules("path/to/rules.yaml"),
        dataset=context["params"]["dataset"],
        partition_value=context["ds"]
    )
    return summary.dict()

dq_task = PythonOperator(
    task_id="run_dq_checks",
    python_callable=run_dq_checks,
    dag=dag
)
```

## 📊 DQ Rule Categories

| Category | Description | Common Check Types |
|----------|-------------|-------------------|
| **Accuracy** | Correctness of data values | `accuracy_check`, `cross_validation` |
| **Completeness** | Mandatory fields not missing | `not_null`, `not_empty`, `completeness` |
| **Consistency** | Alignment across datasets | `cross_table`, `referential_integrity` |
| **Conformity** | Expected formats/enumerations | `format_check`, `standard_compliance` |
| **Uniqueness** | No duplicate keys/rows | `uniqueness`, `primary_key`, `unique_key` |
| **Freshness** | Data updated within SLA | `freshness_lag`, `freshness_check` |
| **Validity** | Domain-specific constraints | `value_range`, `regex_match`, `enum_check` |
| **Integrity** | Referential correctness | `foreign_key`, `referential_integrity`, `fk_exists` |
| **Anomaly** | Unusual patterns/deviations | `statistical_anomaly`, `outlier_detection` |

## 📚 Documentation

- [Architecture Guide](docs/architecture.md) - Comprehensive system architecture and design
- [Examples](src/schemas/examples/) - Example configurations and use cases
- [Development Guidelines](.cursorrules) - Coding standards and best practices

## 🤝 Contributing

### Development Guidelines

This project follows strict development standards defined in `.cursorrules`:

- **Type Safety**: All code must include type hints
- **Documentation**: Comprehensive docstrings for all public APIs
- **Testing**: Minimum 80% test coverage
- **Code Quality**: PEP 8 compliance with Black formatting

### Getting Started

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes following the coding standards
4. Run tests: `make test`
5. Commit your changes: `git commit -m 'Add amazing feature'`
6. Push to the branch: `git push origin feature/amazing-feature`
7. Open a Pull Request

## 🐛 Troubleshooting

### Common Issues

1. **Import Errors**: Ensure you're using the correct import paths from the `src/` package
2. **Schema Validation Failures**: Check your YAML syntax and rule structure
3. **Engine Initialization Errors**: Verify your Databricks and Spark configuration

### Getting Help

- Check the [Issues](https://github.com/company/dq-accelerator/issues) page
- Review the [Architecture Guide](docs/architecture.md)
- Contact the Data Quality Team: dq-team@company.com

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.