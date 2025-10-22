"""
Unit tests for WatermarkManager.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from src.utils.watermark_manager import WatermarkManager, WatermarkRecord


class TestWatermarkRecord:
    """Test cases for WatermarkRecord class."""
    
    def test_watermark_record_creation(self):
        """Test WatermarkRecord creation."""
        record = WatermarkRecord(
            dataset="silver.payments",
            watermark_column="event_date",
            watermark_value="2025-01-15",
            dq_run_completed_ts=datetime(2025, 1, 15, 10, 30, 0)
        )
        
        assert record.dataset == "silver.payments"
        assert record.watermark_column == "event_date"
        assert record.watermark_value == "2025-01-15"
        assert record.dq_run_completed_ts == datetime(2025, 1, 15, 10, 30, 0)
        assert record.updated_ts is not None
    
    def test_watermark_record_to_dict(self):
        """Test WatermarkRecord to_dict method."""
        record = WatermarkRecord(
            dataset="silver.payments",
            watermark_column="event_date",
            watermark_value="2025-01-15",
            dq_run_completed_ts=datetime(2025, 1, 15, 10, 30, 0)
        )
        
        result = record.to_dict()
        
        assert result["dataset"] == "silver.payments"
        assert result["watermark_column"] == "event_date"
        assert result["watermark_value"] == "2025-01-15"
        assert result["dq_run_completed_ts"] == datetime(2025, 1, 15, 10, 30, 0)
        assert "updated_ts" in result


class TestWatermarkManager:
    """Test cases for WatermarkManager class."""
    
    @pytest.fixture
    def mock_spark(self):
        """Create a mock Spark session."""
        spark = Mock(spec=SparkSession)
        spark.read = Mock()
        spark.createDataFrame = Mock()
        return spark
    
    @pytest.fixture
    def watermark_manager(self, mock_spark):
        """Create a WatermarkManager instance with mocked dependencies."""
        with patch.object(WatermarkManager, '_ensure_watermark_table_exists'):
            manager = WatermarkManager(
                spark=mock_spark,
                watermark_table_path="/tmp/test_watermarks"
            )
        return manager
    
    def test_init(self, mock_spark):
        """Test WatermarkManager initialization."""
        with patch.object(WatermarkManager, '_ensure_watermark_table_exists'):
            manager = WatermarkManager(
                spark=mock_spark,
                watermark_table_path="/tmp/test_watermarks"
            )
        
        assert manager.spark == mock_spark
        assert manager.watermark_table_path == "/tmp/test_watermarks"
        assert manager.logger is not None
    
    def test_get_watermark_not_found(self, watermark_manager):
        """Test getting watermark when none exists."""
        # Mock empty result
        mock_df = Mock()
        mock_df.filter.return_value.collect.return_value = []
        watermark_manager.spark.read.format.return_value.load.return_value = mock_df
        
        # Mock the col function to avoid Spark context issues
        with patch('src.utils.watermark_manager.col') as mock_col:
            mock_col.return_value = Mock()
            result = watermark_manager.get_watermark("nonexistent.dataset")
        
        assert result is None
    
    def test_get_watermark_found(self, watermark_manager):
        """Test getting watermark when it exists."""
        # Mock record
        mock_record = Mock()
        mock_record.__getitem__ = Mock(side_effect=lambda key: {
            "dataset": "silver.payments",
            "watermark_column": "event_date",
            "watermark_value": "2025-01-15",
            "dq_run_completed_ts": datetime(2025, 1, 15, 10, 30, 0),
            "updated_ts": datetime(2025, 1, 15, 10, 30, 0)
        }[key])
        
        # Mock DataFrame operations
        mock_df = Mock()
        mock_df.filter.return_value.collect.return_value = [mock_record]
        watermark_manager.spark.read.format.return_value.load.return_value = mock_df
        
        # Mock the col function to avoid Spark context issues
        with patch('src.utils.watermark_manager.col') as mock_col:
            mock_col.return_value = Mock()
            result = watermark_manager.get_watermark("silver.payments")
        
        assert result is not None
        assert isinstance(result, WatermarkRecord)
        assert result.dataset == "silver.payments"
        assert result.watermark_value == "2025-01-15"
    
    def test_set_watermark(self, watermark_manager):
        """Test setting watermark."""
        # Mock DataFrame operations
        mock_df = Mock()
        mock_df.filter.return_value.union.return_value = mock_df
        watermark_manager.spark.read.format.return_value.load.return_value = mock_df
        watermark_manager.spark.createDataFrame.return_value = mock_df
        
        # Mock write operations - create a proper chain
        mock_write = Mock()
        mock_df.write = mock_write
        mock_write.format.return_value = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.option.return_value = mock_write
        mock_write.saveAsTable.return_value = None
        
        # Mock the col function to avoid Spark context issues
        with patch('src.utils.watermark_manager.col') as mock_col:
            mock_col.return_value = Mock()
            # Test setting watermark
            watermark_manager.set_watermark(
                dataset="silver.payments",
                watermark_column="event_date",
                watermark_value="2025-01-16"
            )
        
        # Verify createDataFrame was called
        watermark_manager.spark.createDataFrame.assert_called_once()
        
        # Verify write operations were called
        mock_write.format.assert_called_with("delta")
        mock_write.mode.assert_called_with("overwrite")
    
    def test_get_all_watermarks(self, watermark_manager):
        """Test getting all watermarks."""
        # Mock records
        mock_record1 = Mock()
        mock_record1.__getitem__ = Mock(side_effect=lambda key: {
            "dataset": "silver.payments",
            "watermark_column": "event_date",
            "watermark_value": "2025-01-15",
            "dq_run_completed_ts": datetime(2025, 1, 15, 10, 30, 0),
            "updated_ts": datetime(2025, 1, 15, 10, 30, 0)
        }[key])
        
        mock_record2 = Mock()
        mock_record2.__getitem__ = Mock(side_effect=lambda key: {
            "dataset": "bronze.customers",
            "watermark_column": "load_date",
            "watermark_value": "2025-01-14",
            "dq_run_completed_ts": datetime(2025, 1, 14, 15, 45, 0),
            "updated_ts": datetime(2025, 1, 14, 15, 45, 0)
        }[key])
        
        # Mock DataFrame operations
        mock_df = Mock()
        mock_df.collect.return_value = [mock_record1, mock_record2]
        watermark_manager.spark.read.format.return_value.load.return_value = mock_df
        
        result = watermark_manager.get_all_watermarks()
        
        assert len(result) == 2
        assert all(isinstance(wm, WatermarkRecord) for wm in result)
        assert result[0].dataset == "silver.payments"
        assert result[1].dataset == "bronze.customers"
    
    def test_delete_watermark(self, watermark_manager):
        """Test deleting watermark."""
        # Mock DataFrame operations
        mock_df = Mock()
        mock_df.filter.return_value = mock_df
        watermark_manager.spark.read.format.return_value.load.return_value = mock_df
        
        # Mock write operations - create a proper chain
        mock_write = Mock()
        mock_df.write = mock_write
        mock_write.format.return_value = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.option.return_value = mock_write
        mock_write.saveAsTable.return_value = None
        
        # Mock the col function to avoid Spark context issues
        with patch('src.utils.watermark_manager.col') as mock_col:
            mock_col.return_value = Mock()
            # Test deleting watermark
            watermark_manager.delete_watermark("silver.payments")
        
        # Verify filter was called to exclude the dataset
        mock_df.filter.assert_called_once()
        
        # Verify write operations were called
        mock_write.format.assert_called_with("delta")
        mock_write.mode.assert_called_with("overwrite")
    
    def test_get_watermark_table_info(self, watermark_manager):
        """Test getting watermark table information."""
        # Mock DataFrame operations
        mock_df = Mock()
        mock_df.count.return_value = 2
        mock_df.select.return_value.distinct.return_value.collect.return_value = [
            Mock(__getitem__=Mock(return_value="silver.payments")),
            Mock(__getitem__=Mock(return_value="bronze.customers"))
        ]
        mock_df.schema.json.return_value = '{"type": "struct", "fields": []}'
        watermark_manager.spark.read.format.return_value.load.return_value = mock_df
        
        result = watermark_manager.get_watermark_table_info()
        
        assert result["table_path"] == "/tmp/test_watermarks"
        assert result["total_records"] == 2
        assert "silver.payments" in result["datasets"]
        assert "bronze.customers" in result["datasets"]
        assert "schema" in result


class TestWatermarkManagerIntegration:
    """Integration tests for WatermarkManager (requires Spark)."""
    
    @pytest.mark.integration
    def test_watermark_manager_with_spark(self):
        """Test WatermarkManager with real Spark session."""
        # This test would require a real Spark session
        # For now, we'll skip it in unit tests
        pytest.skip("Integration test - requires Spark session")
    
    @pytest.mark.integration
    def test_watermark_table_creation(self):
        """Test watermark table creation with real Spark."""
        # This test would require a real Spark session
        # For now, we'll skip it in unit tests
        pytest.skip("Integration test - requires Spark session")
