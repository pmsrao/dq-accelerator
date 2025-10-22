"""
Databricks Simple Example

This example demonstrates the simplified Databricks-specific DQ Accelerator.
It assumes 'spark' is available globally and focuses on Databricks features.
"""

import logging
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Import Databricks-specific components
from src.libraries.dq_runner.databricks_runner import DQRunner
from src.libraries.utils.metrics_mart_populator import MetricsMartPopulator


def create_sample_data():
    """Create sample data for testing (assumes spark is available globally)."""
    
    # In Databricks, 'spark' is available globally
    try:
        import builtins
        spark = builtins.spark
    except:
        import sys
        frame = sys._getframe(1)
        spark = frame.f_globals['spark']
    
    # Sample payments data
    payments_schema = StructType([
        StructField("payment_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("amount", IntegerType(), False),
        StructField("event_date", StringType(), False),
        StructField("ingestion_ts", TimestampType(), False)
    ])
    
    payments_data = [
        ("pay_001", "acc_001", 100, "2025-01-15", datetime(2025, 1, 15, 10, 0, 0)),
        ("pay_002", "acc_002", 200, "2025-01-15", datetime(2025, 1, 15, 10, 30, 0)),
        ("pay_003", "acc_001", 150, "2025-01-16", datetime(2025, 1, 16, 9, 0, 0)),
        ("pay_004", "acc_003", 300, "2025-01-16", datetime(2025, 1, 16, 9, 30, 0)),
        ("pay_005", "acc_002", 250, "2025-01-17", datetime(2025, 1, 17, 8, 0, 0)),
        ("pay_006", "acc_001", 175, "2025-01-17", datetime(2025, 1, 17, 8, 30, 0)),
    ]
    
    payments_df = spark.createDataFrame(payments_data, payments_schema)
    payments_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", "/tmp/silver_payments") \
        .saveAsTable("silver.payments")
    
    print("✅ Created sample payments data")


def run_databricks_dq_example():
    """Run the Databricks DQ example."""
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        print("🚀 Starting Databricks DQ Accelerator Example\n")
        
        # 1. Create sample data
        print("1️⃣ Creating sample data...")
        create_sample_data()
        
        # 2. Initialize Databricks DQ Runner
        print("\n2️⃣ Initializing Databricks DQ Runner...")
        dq_runner = DQRunner(
            watermark_table_path="/tmp/dq_watermarks"
        )
        print("✅ Databricks DQ Runner initialized")
        
        # 3. Run incremental DQ processing
        print("\n3️⃣ Running incremental DQ processing...")
        
        rules_file = Path(__file__).parent / "payments_rules.yaml"
        
        # First run - process all data
        print("   📊 First run - processing all available data...")
        summary1 = dq_runner.run_incremental(
            rule_file_path=str(rules_file),
            dataset="silver.payments",
            watermark_column="event_date"
        )
        
        print(f"   ✅ Results: {summary1.passed_rules}/{summary1.total_rules} rules passed")
        print(f"   ⏱️  Execution time: {summary1.execution_time_seconds:.2f}s")
        
        # 4. Add more data and run again
        print("\n4️⃣ Adding more data and running incremental processing...")
        
        # Add new data
        try:
            import builtins
            spark = builtins.spark
        except:
            import sys
            frame = sys._getframe(1)
            spark = frame.f_globals['spark']
        
        new_data = [
            ("pay_007", "acc_004", 400, "2025-01-18", datetime(2025, 1, 18, 10, 0, 0)),
            ("pay_008", "acc_002", 275, "2025-01-18", datetime(2025, 1, 18, 10, 30, 0)),
        ]
        
        new_df = spark.createDataFrame(new_data, spark.table("silver.payments").schema)
        new_df.write \
            .format("delta") \
            .mode("append") \
            .option("path", "/tmp/silver_payments") \
            .saveAsTable("silver.payments")
        
        print("   ✅ Added new data for 2025-01-18")
        
        # Second run - process only new data
        print("\n   📊 Second run - processing only new data...")
        summary2 = dq_runner.run_incremental(
            rule_file_path=str(rules_file),
            dataset="silver.payments",
            watermark_column="event_date"
        )
        
        print(f"   ✅ Results: {summary2.passed_rules}/{summary2.total_rules} rules passed")
        print(f"   ⏱️  Execution time: {summary2.execution_time_seconds:.2f}s")
        
        # 5. Show watermarks
        print("\n5️⃣ Current watermarks...")
        all_watermarks = dq_runner.watermark_manager.get_all_watermarks()
        for wm in all_watermarks:
            print(f"   📍 {wm.dataset}: {wm.watermark_value} (updated: {wm.updated_ts})")
        
        # 6. Generate metrics (if metrics mart is available)
        print("\n6️⃣ Generating metrics...")
        try:
            metrics_populator = MetricsMartPopulator(
                spark_session=spark,
                metrics_mart_path="/tmp/dq_metrics_mart"
            )
            
            # Simulate some DQ results for metrics
            sample_results = [
                {
                    "rule_id": "payments.amount.non_negative.v01",
                    "pass_flag": True,
                    "engine": "soda",
                    "execution_time_ms": 150,
                    "dq_category": "validity"
                },
                {
                    "rule_id": "payments.pk.unique.v01",
                    "pass_flag": True,
                    "engine": "soda",
                    "execution_time_ms": 200,
                    "dq_category": "uniqueness"
                }
            ]
            
            metrics_populator.populate_metrics_from_results(
                dq_results=sample_results,
                dataset="silver.payments",
                execution_date=datetime.now()
            )
            
            # Generate health summary
            health = metrics_populator.get_dataset_health_summary("silver.payments")
            print(f"   📈 Health Score: {health['health_score']}/100")
            print(f"   🏥 Health Status: {health['health_status']}")
            
        except Exception as e:
            print(f"   ⚠️  Metrics generation failed: {e}")
        
        print("\n🎉 Databricks DQ example completed successfully!")
        print("\n📋 Key Benefits of Databricks-Specific Approach:")
        print("   ✅ Simplified - assumes 'spark' is available globally")
        print("   ✅ No complex Spark session management")
        print("   ✅ Optimized for Databricks environment")
        print("   ✅ Cleaner, more maintainable code")
        print("   ✅ Better performance in Databricks")
        
    except Exception as e:
        logger.error(f"Example failed: {e}")
        raise


if __name__ == "__main__":
    run_databricks_dq_example()
