"""
Incremental Processing Example

This example demonstrates how to use the DQ Runner with WatermarkManager
for incremental data quality processing.
"""

import logging
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from src.dq_runner.runner import DQRunner
from src.utils.watermark_manager import WatermarkManager


def setup_spark_session() -> SparkSession:
    """Set up Spark session with Delta Lake support."""
    return SparkSession.builder \
        .appName("DQIncrementalProcessing") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def create_sample_data(spark: SparkSession, table_name: str):
    """Create sample data for testing incremental processing."""
    
    # Sample data schema
    schema = StructType([
        StructField("payment_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("amount", IntegerType(), False),
        StructField("event_date", StringType(), False),
        StructField("ingestion_ts", TimestampType(), False)
    ])
    
    # Sample data
    sample_data = [
        ("pay_001", "acc_001", 100, "2025-01-15", datetime(2025, 1, 15, 10, 0, 0)),
        ("pay_002", "acc_002", 200, "2025-01-15", datetime(2025, 1, 15, 10, 30, 0)),
        ("pay_003", "acc_001", 150, "2025-01-16", datetime(2025, 1, 16, 9, 0, 0)),
        ("pay_004", "acc_003", 300, "2025-01-16", datetime(2025, 1, 16, 9, 30, 0)),
        ("pay_005", "acc_002", 250, "2025-01-17", datetime(2025, 1, 17, 8, 0, 0)),
        ("pay_006", "acc_001", 175, "2025-01-17", datetime(2025, 1, 17, 8, 30, 0)),
    ]
    
    # Create DataFrame and save as Delta table
    df = spark.createDataFrame(sample_data, schema)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", f"/tmp/{table_name}") \
        .saveAsTable(table_name)
    
    print(f"Created sample data in table: {table_name}")
    return df


def run_incremental_processing_example():
    """Run incremental processing example."""
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Initialize Spark session
    spark = setup_spark_session()
    
    try:
        # Create sample data
        table_name = "silver.payments"
        create_sample_data(spark, table_name)
        
        # Initialize WatermarkManager
        watermark_manager = WatermarkManager(
            spark=spark,
            watermark_table_path="/tmp/dq_watermarks"
        )
        
        # Initialize DQ Runner with WatermarkManager
        dq_runner = DQRunner(
            config={
                "data_source": {
                    "type": "spark",
                    "partition_column": "event_date"
                }
            },
            watermark_manager=watermark_manager
        )
        
        # Path to rules file
        rules_file = Path(__file__).parent / "payments_rules.yaml"
        
        print("=== Incremental Processing Example ===\n")
        
        # First run - process all available data
        print("1. First run - processing all available data...")
        summary1 = dq_runner.run_incremental(
            rule_file_path=str(rules_file),
            dataset=table_name,
            partition_column="event_date",
            max_partitions=2  # Process only 2 partitions at a time
        )
        
        print(f"   Results: {summary1.passed_rules}/{summary1.total_rules} rules passed")
        print(f"   Execution time: {summary1.execution_time_seconds:.2f}s")
        
        # Check watermark
        watermark = watermark_manager.get_watermark(table_name)
        if watermark:
            print(f"   Watermark updated to: {watermark.watermark_value}")
        
        # Add more data
        print("\n2. Adding more data...")
        new_data = [
            ("pay_007", "acc_004", 400, "2025-01-18", datetime(2025, 1, 18, 10, 0, 0)),
            ("pay_008", "acc_002", 275, "2025-01-18", datetime(2025, 1, 18, 10, 30, 0)),
        ]
        
        new_df = spark.createDataFrame(new_data, spark.table(table_name).schema)
        new_df.write \
            .format("delta") \
            .mode("append") \
            .option("path", f"/tmp/{table_name}") \
            .saveAsTable(table_name)
        
        print("   Added new data for 2025-01-18")
        
        # Second run - process only new data
        print("\n3. Second run - processing only new data...")
        summary2 = dq_runner.run_incremental(
            rule_file_path=str(rules_file),
            dataset=table_name,
            partition_column="event_date"
        )
        
        print(f"   Results: {summary2.passed_rules}/{summary2.total_rules} rules passed")
        print(f"   Execution time: {summary2.execution_time_seconds:.2f}s")
        
        # Check updated watermark
        watermark = watermark_manager.get_watermark(table_name)
        if watermark:
            print(f"   Watermark updated to: {watermark.watermark_value}")
        
        # Show all watermarks
        print("\n4. All watermarks:")
        all_watermarks = watermark_manager.get_all_watermarks()
        for wm in all_watermarks:
            print(f"   {wm.dataset}: {wm.watermark_value} (updated: {wm.updated_ts})")
        
        print("\n=== Incremental Processing Example Completed ===")
        
    except Exception as e:
        logger.error(f"Example failed: {e}")
        raise
    
    finally:
        spark.stop()


def run_full_processing_example():
    """Run full processing example (without incremental processing)."""
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Initialize Spark session
    spark = setup_spark_session()
    
    try:
        # Create sample data
        table_name = "silver.payments"
        create_sample_data(spark, table_name)
        
        # Initialize DQ Runner without WatermarkManager
        dq_runner = DQRunner(
            config={
                "data_source": {
                    "type": "spark",
                    "partition_column": "event_date"
                }
            }
        )
        
        # Path to rules file
        rules_file = Path(__file__).parent / "payments_rules.yaml"
        
        print("=== Full Processing Example ===\n")
        
        # Run DQ rules for all data
        print("Running DQ rules for all data...")
        results, summary = dq_runner.run(str(rules_file))
        
        print(f"Results: {summary.passed_rules}/{summary.total_rules} rules passed")
        print(f"Execution time: {summary.execution_time_seconds:.2f}s")
        
        # Show detailed results
        print("\nDetailed results:")
        for result in results:
            status = "PASS" if result.pass_flag else "FAIL"
            print(f"  {result.rule_id}: {status} ({result.execution_time_ms}ms)")
            if result.error_message:
                print(f"    Error: {result.error_message}")
        
        print("\n=== Full Processing Example Completed ===")
        
    except Exception as e:
        logger.error(f"Example failed: {e}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    print("Choose example to run:")
    print("1. Incremental Processing Example")
    print("2. Full Processing Example")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        run_incremental_processing_example()
    elif choice == "2":
        run_full_processing_example()
    else:
        print("Invalid choice. Running incremental processing example by default.")
        run_incremental_processing_example()
