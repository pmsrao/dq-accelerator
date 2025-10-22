"""
Watermark Manager for Data Quality Accelerator.

Manages high water marks (HWM) for incremental data processing using Delta Lake tables.
Tracks the last successfully processed partition value for each dataset to enable
efficient incremental processing.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


class WatermarkRecord:
    """Represents a watermark record for a dataset."""
    
    def __init__(
        self,
        dataset: str,
        watermark_column: str,
        watermark_value: str,
        dq_run_completed_ts: datetime,
        updated_ts: Optional[datetime] = None
    ):
        self.dataset = dataset
        self.watermark_column = watermark_column
        self.watermark_value = watermark_value
        self.dq_run_completed_ts = dq_run_completed_ts
        self.updated_ts = updated_ts or datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame operations."""
        return {
            "dataset": self.dataset,
            "watermark_column": self.watermark_column,
            "watermark_value": self.watermark_value,
            "dq_run_completed_ts": self.dq_run_completed_ts,
            "updated_ts": self.updated_ts
        }


class WatermarkManager:
    """
    Manages high water marks for incremental data processing.
    
    Uses a Delta Lake table to persist watermark information for each dataset,
    enabling efficient incremental processing by tracking the last successfully
    processed partition value.
    """
    
    # Schema for watermark table
    WATERMARK_SCHEMA = StructType([
        StructField("dataset", StringType(), False),
        StructField("watermark_column", StringType(), False),
        StructField("watermark_value", StringType(), False),
        StructField("dq_run_completed_ts", TimestampType(), False),
        StructField("updated_ts", TimestampType(), False)
    ])
    
    def __init__(
        self,
        spark: SparkSession,
        watermark_table_path: str,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the WatermarkManager.
        
        Args:
            spark: Spark session for DataFrame operations
            watermark_table_path: Path to the Delta table for storing watermarks
            logger: Optional logger instance
        """
        self.spark = spark
        self.watermark_table_path = watermark_table_path
        self.logger = logger or self._setup_logger()
        
        # Initialize the watermark table if it doesn't exist
        self._ensure_watermark_table_exists()
    
    def _setup_logger(self) -> logging.Logger:
        """Set up logger for the WatermarkManager."""
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def _ensure_watermark_table_exists(self) -> None:
        """Ensure the watermark table exists, create if it doesn't."""
        try:
            # Try to read the table to check if it exists
            self.spark.read.format("delta").load(self.watermark_table_path).limit(1).collect()
            self.logger.info(f"Watermark table exists at {self.watermark_table_path}")
        except Exception:
            # Table doesn't exist, create it
            self.logger.info(f"Creating watermark table at {self.watermark_table_path}")
            self._create_watermark_table()
    
    def _create_watermark_table(self) -> None:
        """Create the watermark table with the defined schema."""
        try:
            # Create empty DataFrame with the schema
            empty_df = self.spark.createDataFrame([], self.WATERMARK_SCHEMA)
            
            # Write as Delta table
            empty_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("path", self.watermark_table_path) \
                .saveAsTable("watermark_table")
            
            self.logger.info("Watermark table created successfully")
        except Exception as e:
            self.logger.error(f"Failed to create watermark table: {e}")
            raise
    
    def get_watermark(self, dataset: str) -> Optional[WatermarkRecord]:
        """
        Get the watermark for a specific dataset.
        
        Args:
            dataset: Dataset name to get watermark for
            
        Returns:
            WatermarkRecord if found, None otherwise
        """
        try:
            df = self.spark.read.format("delta").load(self.watermark_table_path)
            
            # Filter for the specific dataset
            watermark_df = df.filter(col("dataset") == dataset)
            
            # Collect results
            records = watermark_df.collect()
            
            if not records:
                self.logger.info(f"No watermark found for dataset: {dataset}")
                return None
            
            # Get the most recent record (should be only one per dataset)
            record = records[0]
            
        watermark = WatermarkRecord(
            dataset=record["dataset"],
            watermark_column=record["watermark_column"],
            watermark_value=record["watermark_value"],
            dq_run_completed_ts=record["dq_run_completed_ts"],
            updated_ts=record["updated_ts"]
        )
        
        self.logger.info(f"Retrieved watermark for {dataset}: {watermark.watermark_value}")
        return watermark
            
        except Exception as e:
            self.logger.error(f"Failed to get watermark for dataset {dataset}: {e}")
            raise
    
    def set_watermark(
        self,
        dataset: str,
        watermark_column: str,
        watermark_value: str,
        dq_run_completed_ts: Optional[datetime] = None
    ) -> None:
        """
        Set or update the watermark for a dataset.
        
        Args:
            dataset: Dataset name
            watermark_column: Name of the watermark column
            watermark_value: Last successfully processed watermark value
            dq_run_completed_ts: Timestamp when DQ run completed successfully
        """
        try:
            if dq_run_completed_ts is None:
                dq_run_completed_ts = datetime.utcnow()
            
            # Create new watermark record
            new_watermark = WatermarkRecord(
                dataset=dataset,
                watermark_column=watermark_column,
                watermark_value=watermark_value,
                dq_run_completed_ts=dq_run_completed_ts
            )
            
            # Create DataFrame with the new record
            watermark_df = self.spark.createDataFrame([new_watermark.to_dict()], self.WATERMARK_SCHEMA)
            
            # Read existing table
            existing_df = self.spark.read.format("delta").load(self.watermark_table_path)
            
            # Remove existing record for this dataset and add new one
            filtered_df = existing_df.filter(col("dataset") != dataset)
            combined_df = filtered_df.union(watermark_df)
            
            # Write back to Delta table
            combined_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("path", self.watermark_table_path) \
                .saveAsTable("watermark_table")
            
            self.logger.info(f"Updated watermark for {dataset}: {watermark_value}")
            
        except Exception as e:
            self.logger.error(f"Failed to set watermark for dataset {dataset}: {e}")
            raise
    
    def get_all_watermarks(self) -> List[WatermarkRecord]:
        """
        Get all watermarks from the table.
        
        Returns:
            List of WatermarkRecord objects
        """
        try:
            df = self.spark.read.format("delta").load(self.watermark_table_path)
            records = df.collect()
            
            watermarks = []
            for record in records:
                watermark = WatermarkRecord(
                    dataset=record["dataset"],
                    watermark_column=record["watermark_column"],
                    watermark_value=record["watermark_value"],
                    dq_run_completed_ts=record["dq_run_completed_ts"],
                    updated_ts=record["updated_ts"]
                )
                watermarks.append(watermark)
            
            self.logger.info(f"Retrieved {len(watermarks)} watermarks")
            return watermarks
            
        except Exception as e:
            self.logger.error(f"Failed to get all watermarks: {e}")
            raise
    
    def delete_watermark(self, dataset: str) -> None:
        """
        Delete watermark for a specific dataset.
        
        Args:
            dataset: Dataset name to delete watermark for
        """
        try:
            # Read existing table
            df = self.spark.read.format("delta").load(self.watermark_table_path)
            
            # Filter out the dataset
            filtered_df = df.filter(col("dataset") != dataset)
            
            # Write back to Delta table
            filtered_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("path", self.watermark_table_path) \
                .saveAsTable("watermark_table")
            
            self.logger.info(f"Deleted watermark for dataset: {dataset}")
            
        except Exception as e:
            self.logger.error(f"Failed to delete watermark for dataset {dataset}: {e}")
            raise
    
    def get_watermark_table_info(self) -> Dict[str, Any]:
        """
        Get information about the watermark table.
        
        Returns:
            Dictionary with table information
        """
        try:
            df = self.spark.read.format("delta").load(self.watermark_table_path)
            
            # Get basic statistics
            total_records = df.count()
            datasets = [row["dataset"] for row in df.select("dataset").distinct().collect()]
            
            info = {
                "table_path": self.watermark_table_path,
                "total_records": total_records,
                "datasets": datasets,
                "schema": df.schema.json()
            }
            
            return info
            
        except Exception as e:
            self.logger.error(f"Failed to get watermark table info: {e}")
            raise


# Convenience functions for backward compatibility
def get_watermark(watermark_manager: WatermarkManager, dataset: str) -> Optional[Dict[str, Any]]:
    """Get watermark as dictionary for backward compatibility."""
    watermark = watermark_manager.get_watermark(dataset)
    return watermark.to_dict() if watermark else None


def set_watermark(
    watermark_manager: WatermarkManager,
    dataset: str,
    watermark_column: str,
    watermark_value: str,
    dq_run_completed_ts: Optional[datetime] = None
) -> None:
    """Set watermark for backward compatibility."""
    watermark_manager.set_watermark(dataset, watermark_column, watermark_value, dq_run_completed_ts)
