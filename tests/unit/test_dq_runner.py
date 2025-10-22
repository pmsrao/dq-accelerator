"""
Unit tests for DQ Runner module.
"""

import pytest
from unittest.mock import Mock, patch
from src.dq_runner.runner import DQRunner, DQResult, DQRunSummary


class TestDQRunner:
    """Test cases for DQRunner class."""
    
    def test_init(self):
        """Test DQRunner initialization."""
        runner = DQRunner()
        assert runner.config == {}
        assert runner.soda_engine is None
        assert runner.sql_engine is None
        
    def test_init_with_config(self):
        """Test DQRunner initialization with config."""
        config = {"test": "value"}
        runner = DQRunner(config=config)
        assert runner.config == config
        
    def test_filter_rules_by_engine(self):
        """Test filtering rules by engine."""
        runner = DQRunner()
        rules = [
            {"id": "rule1", "engine": "soda"},
            {"id": "rule2", "engine": "sql"},
            {"id": "rule3", "engine": "soda"}
        ]
        
        soda_rules = runner.filter_rules_by_engine(rules, "soda")
        assert len(soda_rules) == 2
        assert all(rule["engine"] == "soda" for rule in soda_rules)
        
        sql_rules = runner.filter_rules_by_engine(rules, "sql")
        assert len(sql_rules) == 1
        assert sql_rules[0]["engine"] == "sql"


class TestDQResult:
    """Test cases for DQResult model."""
    
    def test_dq_result_creation(self):
        """Test DQResult model creation."""
        result = DQResult(
            rule_id="test.rule.v01",
            pass_flag=True,
            engine="soda",
            execution_time_ms=100
        )
        assert result.rule_id == "test.rule.v01"
        assert result.pass_flag is True
        assert result.engine == "soda"
        assert result.execution_time_ms == 100
        assert result.error_message is None


class TestDQRunSummary:
    """Test cases for DQRunSummary model."""
    
    def test_dq_run_summary_creation(self):
        """Test DQRunSummary model creation."""
        summary = DQRunSummary(
            run_id="test-run-123",
            dataset="test.dataset",
            partition_value="2025-01-01",
            started_ts="2025-01-01 10:00:00",
            ended_ts="2025-01-01 10:01:00",
            total_rules=10,
            passed_rules=8,
            failed_rules=2,
            execution_time_seconds=60.0
        )
        assert summary.run_id == "test-run-123"
        assert summary.dataset == "test.dataset"
        assert summary.total_rules == 10
        assert summary.passed_rules == 8
        assert summary.failed_rules == 2
