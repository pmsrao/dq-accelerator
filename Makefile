# Data Quality Accelerator Makefile

.PHONY: help install install-dev test test-unit test-integration test-e2e lint format clean build docs run-example

# Default target
help:
	@echo "Data Quality Accelerator - Available targets:"
	@echo ""
	@echo "Development:"
	@echo "  install        - Install production dependencies"
	@echo "  install-dev    - Install development dependencies"
	@echo "  format         - Format code with black"
	@echo "  lint           - Run linting (flake8, mypy)"
	@echo "  clean          - Clean build artifacts"
	@echo ""
	@echo "Testing:"
	@echo "  test           - Run all tests"
	@echo "  test-unit      - Run unit tests only"
	@echo "  test-integration - Run integration tests only"
	@echo "  test-e2e       - Run end-to-end tests only"
	@echo "  test-coverage  - Run tests with coverage report"
	@echo ""
	@echo "Build & Deploy:"
	@echo "  build          - Build package"
	@echo "  docs           - Generate documentation"
	@echo ""
	@echo "Examples:"
	@echo "  run-example    - Run example DQ rules"
	@echo "  validate-rules - Validate example rules"

# Installation targets
install:
	pip install -e .

install-dev:
	pip install -e ".[dev,test]"
	pre-commit install

# Code quality targets
format:
	black src/ tests/ examples/
	isort src/ tests/ examples/

lint:
	flake8 src/ tests/
	mypy src/
	black --check src/ tests/ examples/
	isort --check-only src/ tests/ examples/

# Testing targets
test:
	pytest tests/ -v

test-unit:
	pytest tests/unit/ -v -m "not integration and not e2e"

test-integration:
	pytest tests/integration/ -v -m integration

test-e2e:
	pytest tests/e2e/ -v -m e2e

test-coverage:
	pytest tests/ --cov=src --cov-report=html --cov-report=term-missing

# Build targets
build:
	python -m build

docs:
	@echo "Documentation generation not yet implemented"

# Example targets
run-example:
	python -m src.dq_runner.runner --rule-file src/schemas/examples/payments_rules.yaml --verbose

validate-rules:
	python -c "from src.dq_runner.compliance_checker import check_compliance; print('\\n'.join(check_compliance('src/schemas/dq_rule_schema.json', 'src/schemas/examples/payments_rules.yaml')))"

# Cleanup targets
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	find . -type d -name ".coverage" -exec rm -rf {} +
	find . -type d -name "htmlcov" -exec rm -rf {} +
	find . -type d -name "dist" -exec rm -rf {} +
	find . -type d -name "build" -exec rm -rf {} +

# CI/CD targets
ci-test: install-dev lint test-coverage

# Development workflow
dev-setup: install-dev
	@echo "Development environment setup complete!"
	@echo "Run 'make help' to see available commands"

# Quick development cycle
quick-check: format lint test-unit
