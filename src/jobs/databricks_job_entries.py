"""
Databricks Job Entry Points - Simplified for Databricks

This module provides simplified entry points for Databricks job execution,
specifically designed for Databricks environments with 'spark' available globally.
"""

import argparse
import logging
import sys
from pathlib import Path

from ..libraries.dq_runner.databricks_runner import DQRunner


def setup_logging() -> logging.Logger:
    """Set up logging for Databricks job execution."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def run_incremental_job():
    """Entry point for incremental DQ job execution in Databricks."""
    parser = argparse.ArgumentParser(description="Incremental DQ Job Execution (Databricks)")
    parser.add_argument("--rules-path", required=True, help="Path to DQ rules YAML file or folder")
    parser.add_argument("--dataset", required=True, help="Dataset name to process")
    parser.add_argument("--watermark-column", required=True, help="Watermark column name")
    parser.add_argument("--watermark-table-name", default="dq_watermarks", 
                       help="Name of watermark table (Unity Catalog managed)")
    parser.add_argument("--environment", help="Environment name for catalog/schema resolution (dev, staging, prod)")
    
    args = parser.parse_args()
    logger = setup_logging()
    
    try:
        logger.info(f"Starting incremental DQ job for dataset: {args.dataset}")
        
        # Initialize Databricks DQ Runner
        dq_runner = DQRunner(
            watermark_table_name=args.watermark_table_name
        )
        logger.info("Databricks DQ Runner initialized")
        
        # Run incremental DQ rules
        summary = dq_runner.run_incremental(
            rule_path=args.rules_path,
            dataset=args.dataset,
            watermark_column=args.watermark_column,
            environment=args.environment
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


def run_full_job():
    """Entry point for full DQ job execution in Databricks."""
    parser = argparse.ArgumentParser(description="Full DQ Job Execution (Databricks)")
    parser.add_argument("--rules-path", required=True, help="Path to DQ rules YAML file or folder")
    parser.add_argument("--dataset", required=True, help="Dataset name to process")
    parser.add_argument("--watermark-table-name", default="dq_watermarks", 
                       help="Name of watermark table (Unity Catalog managed)")
    parser.add_argument("--environment", help="Environment name for catalog/schema resolution (dev, staging, prod)")
    
    args = parser.parse_args()
    logger = setup_logging()
    
    try:
        logger.info(f"Starting full DQ job for dataset: {args.dataset}")
        
        # Initialize Databricks DQ Runner
        dq_runner = DQRunner(
            watermark_table_name=args.watermark_table_name
        )
        logger.info("Databricks DQ Runner initialized")
        
        # For full processing, we'll process all partitions
        # This is a simplified approach - in practice you might want to implement
        # a full processing method that doesn't use watermarks
        
        # Get all partitions and process them
        spark = dq_runner._get_spark_session()
        
        # Get all unique partition values (assuming event_date as default)
        partition_column = "event_date"  # This could be configurable
        partitions_sql = f"""
        SELECT DISTINCT {partition_column}
        FROM {args.dataset}
        ORDER BY {partition_column}
        """
        
        partitions_df = spark.sql(partitions_sql)
        partitions = [row[partition_column] for row in partitions_df.collect()]
        
        logger.info(f"Found {len(partitions)} partitions to process")
        
        # Process each partition
        all_results = []
        for partition in partitions:
            try:
                # Load rules
                rules_spec = dq_runner.load_rules(args.rules_file)
                
                # Run partition
                partition_results = dq_runner._run_partition(rules_spec, args.dataset, partition)
                all_results.extend(partition_results)
                
                logger.info(f"Processed partition: {partition}")
                
            except Exception as e:
                logger.error(f"Failed to process partition {partition}: {e}")
                continue
        
        # Create summary
        summary = dq_runner._create_run_summary(all_results, args.dataset, 0.0)
        
        logger.info(f"Full DQ job completed: {summary.passed_rules}/{summary.total_rules} rules passed")
        
        if summary.failed_rules > 0:
            logger.warning(f"Some rules failed: {summary.failed_rules}")
            sys.exit(1)
        
        logger.info("Full DQ job completed successfully")
        
    except Exception as e:
        logger.error(f"Full DQ job failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # This allows the module to be run directly for testing
    if len(sys.argv) > 1:
        job_type = sys.argv[1]
        if job_type == "incremental":
            run_incremental_job()
        elif job_type == "full":
            run_full_job()
        else:
            print("Unknown job type. Use: incremental or full")
            sys.exit(1)
    else:
        print("Usage: python -m src.dq_runner.databricks_job_entries <job_type>")
        print("Job types: incremental, full")
        sys.exit(1)
