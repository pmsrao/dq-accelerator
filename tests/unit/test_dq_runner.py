"""
Unit tests for DQ Runner module.
"""

import pytest
from unittest.mock import Mock, patch
from src.libraries.dq_runner.databricks_runner import DQRunner, DQResult, DQRunSummary


class TestDQRunner:
    """Test cases for DQRunner class."""
    
    @patch('src.libraries.dq_runner.databricks_runner.DQRunner._init_watermark_manager')
    def test_init(self, mock_init_watermark):
        """Test DQRunner initialization."""
        mock_watermark_manager = Mock()
        mock_init_watermark.return_value = mock_watermark_manager
        
        runner = DQRunner()
        assert runner is not None
        assert runner.soda_engine is None
        assert runner.sql_engine is None
        
    @patch('src.libraries.dq_runner.databricks_runner.DQRunner._init_watermark_manager')
    def test_init_with_watermark_path(self, mock_init_watermark):
        """Test DQRunner initialization with custom watermark path."""
        mock_watermark_manager = Mock()
        mock_init_watermark.return_value = mock_watermark_manager
        
        runner = DQRunner(watermark_table_path="/custom/watermarks")
        assert runner is not None


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
