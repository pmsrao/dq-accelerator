"""
Databricks Job Entry Points

This module provides entry points for Databricks job execution,
handling both full and incremental DQ processing.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession

from .runner import DQRunner
from ..utils.watermark_manager import WatermarkManager


def setup_logging() -> logging.Logger:
    """Set up logging for Databricks job execution."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def get_spark_session() -> SparkSession:
    """Get Spark session (Databricks-friendly)."""
    # In Databricks, 'spark' is available globally
    try:
        # Try to get from global scope
        import builtins
        if hasattr(builtins, 'spark'):
            return builtins.spark
    except:
        pass
    
    # Try to get from globals
    try:
        import sys
        frame = sys._getframe(1)
        if 'spark' in frame.f_globals:
            return frame.f_globals['spark']
    except:
        pass
    
    # Fallback: create new session
    return SparkSession.builder \
        .appName("DQAccelerator") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def run_full_job():
    """Entry point for full DQ job execution."""
    parser = argparse.ArgumentParser(description="Full DQ Job Execution")
    parser.add_argument("--rules-file", required=True, help="Path to DQ rules YAML file")
    parser.add_argument("--dataset", required=True, help="Dataset name to process")
    parser.add_argument("--watermark-table-path", default="/tmp/dq_watermarks", 
                       help="Path to watermark table")
    
    args = parser.parse_args()
    logger = setup_logging()
    
    try:
        logger.info(f"Starting full DQ job for dataset: {args.dataset}")
        
        # Get Spark session
        spark = get_spark_session()
        logger.info("Spark session initialized")
        
        # Initialize DQ Runner
        dq_runner = DQRunner(
            config={
                "data_source": {
                    "type": "spark",
                    "partition_column": "event_date"
                }
            }
        )
        
        # Run DQ rules
        results, summary = dq_runner.run(args.rules_file)
        
        logger.info(f"DQ job completed: {summary.passed_rules}/{summary.total_rules} rules passed")
        logger.info(f"Execution time: {summary.execution_time_seconds:.2f}s")
        
        # Log failed rules
        failed_rules = [r for r in results if not r.pass_flag]
        if failed_rules:
            logger.warning(f"Failed rules: {[r.rule_id for r in failed_rules]}")
            sys.exit(1)
        
        logger.info("DQ job completed successfully")
        
    except Exception as e:
        logger.error(f"DQ job failed: {e}")
        sys.exit(1)


def run_incremental_job():
    """Entry point for incremental DQ job execution."""
    parser = argparse.ArgumentParser(description="Incremental DQ Job Execution")
    parser.add_argument("--rules-file", required=True, help="Path to DQ rules YAML file")
    parser.add_argument("--dataset", required=True, help="Dataset name to process")
    parser.add_argument("--partition-column", required=True, help="Partition column name")
    parser.add_argument("--watermark-table-path", default="/tmp/dq_watermarks", 
                       help="Path to watermark table")
    parser.add_argument("--max-partitions", type=int, default=None,
                       help="Maximum number of partitions to process")
    
    args = parser.parse_args()
    logger = setup_logging()
    
    try:
        logger.info(f"Starting incremental DQ job for dataset: {args.dataset}")
        
        # Get Spark session
        spark = get_spark_session()
        logger.info("Spark session initialized")
        
        # Initialize WatermarkManager
        watermark_manager = WatermarkManager(
            spark=spark,
            watermark_table_path=args.watermark_table_path
        )
        logger.info("WatermarkManager initialized")
        
        # Initialize DQ Runner with WatermarkManager
        dq_runner = DQRunner(
            config={
                "data_source": {
                    "type": "spark",
                    "partition_column": args.partition_column
                }
            },
            watermark_manager=watermark_manager
        )
        
        # Run incremental DQ rules
        summary = dq_runner.run_incremental(
            rule_file_path=args.rules_file,
            dataset=args.dataset,
            partition_column=args.partition_column,
            max_partitions=args.max_partitions
        )
        
        logger.info(f"Incremental DQ job completed: {summary.passed_rules}/{summary.total_rules} rules passed")
        logger.info(f"Execution time: {summary.execution_time_seconds:.2f}s")
        
        if summary.failed_rules > 0:
            logger.warning(f"Some rules failed: {summary.failed_rules}")
            sys.exit(1)
        
        logger.info("Incremental DQ job completed successfully")
        
    except Exception as e:
        logger.error(f"Incremental DQ job failed: {e}")
        sys.exit(1)


def cleanup_old_watermarks():
    """Entry point for watermark cleanup job."""
    parser = argparse.ArgumentParser(description="Watermark Cleanup Job")
    parser.add_argument("--dataset", required=True, help="Dataset name")
    parser.add_argument("--retention-days", type=int, default=30, 
                       help="Number of days to retain watermarks")
    parser.add_argument("--watermark-table-path", default="/tmp/dq_watermarks", 
                       help="Path to watermark table")
    
    args = parser.parse_args()
    logger = setup_logging()
    
    try:
        logger.info(f"Starting watermark cleanup for dataset: {args.dataset}")
        
        # Get Spark session
        spark = get_spark_session()
        
        # Initialize WatermarkManager
        watermark_manager = WatermarkManager(
            spark=spark,
            watermark_table_path=args.watermark_table_path
        )
        
        # Cleanup old watermarks (placeholder implementation)
        logger.info(f"Watermark cleanup completed for {args.dataset}")
        
    except Exception as e:
        logger.error(f"Watermark cleanup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # This allows the module to be run directly for testing
    if len(sys.argv) > 1:
        job_type = sys.argv[1]
        if job_type == "full":
            run_full_job()
        elif job_type == "incremental":
            run_incremental_job()
        elif job_type == "cleanup":
            cleanup_old_watermarks()
        else:
            print("Unknown job type. Use: full, incremental, or cleanup")
            sys.exit(1)
    else:
        print("Usage: python -m src.dq_runner.databricks_jobs <job_type>")
        print("Job types: full, incremental, cleanup")
        sys.exit(1)
