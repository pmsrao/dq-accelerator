"""
Metrics Mart Job Entry Points

This module provides entry points for metrics mart population jobs
that can be scheduled in Databricks workflows.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession

from .metrics_mart_populator import MetricsMartPopulator
from .watermark_manager import WatermarkManager


def setup_logging() -> logging.Logger:
    """Set up logging for metrics job execution."""
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
        .appName("DQMetricsMart") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def populate_daily_metrics():
    """Entry point for daily metrics population job."""
    parser = argparse.ArgumentParser(description="Daily Metrics Population Job")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD), defaults to yesterday")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD), defaults to yesterday")
    parser.add_argument("--metrics-mart-path", default="/tmp/dq_metrics_mart", 
                       help="Path to metrics mart table")
    
    args = parser.parse_args()
    logger = setup_logging()
    
    try:
        logger.info("Starting daily metrics population job")
        
        # Parse dates
        if args.start_date:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        else:
            start_date = datetime.now() - timedelta(days=1)
        
        if args.end_date:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
        else:
            end_date = datetime.now() - timedelta(days=1)
        
        logger.info(f"Processing metrics for date range: {start_date.date()} to {end_date.date()}")
        
        # Get Spark session
        spark = get_spark_session()
        logger.info("Spark session initialized")
        
        # Initialize Metrics Mart Populator
        populator = MetricsMartPopulator(
            spark_session=spark,
            metrics_mart_path=args.metrics_mart_path
        )
        logger.info("Metrics Mart Populator initialized")
        
        # Populate daily metrics
        populator.populate_daily_metrics(start_date=start_date, end_date=end_date)
        
        logger.info("Daily metrics population completed successfully")
        
    except Exception as e:
        logger.error(f"Daily metrics population failed: {e}")
        sys.exit(1)


def generate_health_summary():
    """Entry point for dataset health summary generation."""
    parser = argparse.ArgumentParser(description="Dataset Health Summary Job")
    parser.add_argument("--dataset", required=True, help="Dataset name")
    parser.add_argument("--days", type=int, default=7, help="Number of days to analyze")
    parser.add_argument("--metrics-mart-path", default="/tmp/dq_metrics_mart", 
                       help="Path to metrics mart table")
    parser.add_argument("--output-path", help="Output path for summary JSON")
    
    args = parser.parse_args()
    logger = setup_logging()
    
    try:
        logger.info(f"Generating health summary for dataset: {args.dataset}")
        
        # Get Spark session
        spark = get_spark_session()
        
        # Initialize Metrics Mart Populator
        populator = MetricsMartPopulator(
            spark_session=spark,
            metrics_mart_path=args.metrics_mart_path
        )
        
        # Generate health summary
        summary = populator.get_dataset_health_summary(
            dataset=args.dataset,
            days=args.days
        )
        
        # Output summary
        if args.output_path:
            import json
            with open(args.output_path, 'w') as f:
                json.dump(summary, f, indent=2)
            logger.info(f"Health summary written to: {args.output_path}")
        else:
            import json
            print(json.dumps(summary, indent=2))
        
        logger.info("Health summary generation completed successfully")
        
    except Exception as e:
        logger.error(f"Health summary generation failed: {e}")
        sys.exit(1)


def cleanup_old_metrics():
    """Entry point for metrics cleanup job."""
    parser = argparse.ArgumentParser(description="Metrics Cleanup Job")
    parser.add_argument("--retention-days", type=int, default=90, 
                       help="Number of days to retain metrics")
    parser.add_argument("--metrics-mart-path", default="/tmp/dq_metrics_mart", 
                       help="Path to metrics mart table")
    
    args = parser.parse_args()
    logger = setup_logging()
    
    try:
        logger.info(f"Starting metrics cleanup (retention: {args.retention_days} days)")
        
        # Get Spark session
        spark = get_spark_session()
        
        # Initialize Metrics Mart Populator
        populator = MetricsMartPopulator(
            spark_session=spark,
            metrics_mart_path=args.metrics_mart_path
        )
        
        # Cleanup old metrics
        populator.cleanup_old_metrics(retention_days=args.retention_days)
        
        logger.info("Metrics cleanup completed successfully")
        
    except Exception as e:
        logger.error(f"Metrics cleanup failed: {e}")
        sys.exit(1)


def calculate_trends():
    """Entry point for trend calculation job."""
    parser = argparse.ArgumentParser(description="Trend Calculation Job")
    parser.add_argument("--dataset", required=True, help="Dataset name")
    parser.add_argument("--rule-id", help="Specific rule ID (optional)")
    parser.add_argument("--days", type=int, default=30, help="Number of days to analyze")
    parser.add_argument("--metrics-mart-path", default="/tmp/dq_metrics_mart", 
                       help="Path to metrics mart table")
    parser.add_argument("--output-path", help="Output path for trends CSV")
    
    args = parser.parse_args()
    logger = setup_logging()
    
    try:
        logger.info(f"Calculating trends for dataset: {args.dataset}")
        
        # Get Spark session
        spark = get_spark_session()
        
        # Initialize Metrics Mart Populator
        populator = MetricsMartPopulator(
            spark_session=spark,
            metrics_mart_path=args.metrics_mart_path
        )
        
        # Calculate trends
        trends_df = populator.calculate_trends(
            dataset=args.dataset,
            rule_id=args.rule_id,
            days=args.days
        )
        
        # Output trends
        if args.output_path:
            trends_df.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(args.output_path)
            logger.info(f"Trends written to: {args.output_path}")
        else:
            trends_df.show(truncate=False)
        
        logger.info("Trend calculation completed successfully")
        
    except Exception as e:
        logger.error(f"Trend calculation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # This allows the module to be run directly for testing
    if len(sys.argv) > 1:
        job_type = sys.argv[1]
        if job_type == "daily":
            populate_daily_metrics()
        elif job_type == "health":
            generate_health_summary()
        elif job_type == "cleanup":
            cleanup_old_metrics()
        elif job_type == "trends":
            calculate_trends()
        else:
            print("Unknown job type. Use: daily, health, cleanup, or trends")
            sys.exit(1)
    else:
        print("Usage: python -m src.utils.metrics_jobs <job_type>")
        print("Job types: daily, health, cleanup, trends")
        sys.exit(1)
