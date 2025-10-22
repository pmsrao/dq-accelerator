"""
Complete Workflow Example

This example demonstrates the complete Data Quality Accelerator workflow including:
1. DQ rule execution with both engines
2. Incremental processing with watermarks
3. Metrics mart population
4. Databricks workflow integration
"""

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Import DQ Accelerator components
from src.dq_runner.runner import DQRunner
from src.utils.watermark_manager import WatermarkManager
from src.utils.metrics_mart_populator import MetricsMartPopulator
from src.integrations.databricks_workflow import DatabricksWorkflowManager


def setup_spark_session() -> SparkSession:
    """Set up Spark session with Delta Lake support."""
    return SparkSession.builder \
        .appName("DQAcceleratorCompleteExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def create_sample_data(spark: SparkSession):
    """Create sample data for testing."""
    
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
        ("pay_007", "acc_004", 400, "2025-01-18", datetime(2025, 1, 18, 10, 0, 0)),
        ("pay_008", "acc_002", 275, "2025-01-18", datetime(2025, 1, 18, 10, 30, 0)),
    ]
    
    payments_df = spark.createDataFrame(payments_data, payments_schema)
    payments_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", "/tmp/silver_payments") \
        .saveAsTable("silver.payments")
    
    print("✅ Created sample payments data")
    
    # Sample accounts data
    accounts_schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("account_name", StringType(), False),
        StructField("account_type", StringType(), False),
        StructField("load_date", StringType(), False)
    ])
    
    accounts_data = [
        ("acc_001", "Account 1", "premium", "2025-01-15"),
        ("acc_002", "Account 2", "standard", "2025-01-15"),
        ("acc_003", "Account 3", "premium", "2025-01-16"),
        ("acc_004", "Account 4", "standard", "2025-01-17"),
    ]
    
    accounts_df = spark.createDataFrame(accounts_data, accounts_schema)
    accounts_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", "/tmp/silver_accounts") \
        .saveAsTable("silver.accounts")
    
    print("✅ Created sample accounts data")


def run_complete_workflow_example():
    """Run the complete workflow example."""
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Initialize Spark session
    spark = setup_spark_session()
    
    try:
        print("🚀 Starting Complete DQ Accelerator Workflow Example\n")
        
        # 1. Create sample data
        print("1️⃣ Creating sample data...")
        create_sample_data(spark)
        
        # 2. Initialize components
        print("\n2️⃣ Initializing DQ Accelerator components...")
        
        # Watermark Manager
        watermark_manager = WatermarkManager(
            spark=spark,
            watermark_table_path="/tmp/dq_watermarks"
        )
        print("✅ WatermarkManager initialized")
        
        # Metrics Mart Populator
        metrics_populator = MetricsMartPopulator(
            spark_session=spark,
            metrics_mart_path="/tmp/dq_metrics_mart"
        )
        print("✅ MetricsMartPopulator initialized")
        
        # DQ Runner with WatermarkManager
        dq_runner = DQRunner(
            config={
                "data_source": {
                    "type": "spark",
                    "partition_column": "event_date"
                }
            },
            watermark_manager=watermark_manager
        )
        print("✅ DQRunner initialized")
        
        # 3. Run incremental DQ processing
        print("\n3️⃣ Running incremental DQ processing...")
        
        rules_file = Path(__file__).parent / "payments_rules.yaml"
        
        # First run - process all data
        print("   📊 First run - processing all available data...")
        summary1 = dq_runner.run_incremental(
            rule_file_path=str(rules_file),
            dataset="silver.payments",
            partition_column="event_date",
            max_partitions=2
        )
        
        print(f"   ✅ Results: {summary1.passed_rules}/{summary1.total_rules} rules passed")
        print(f"   ⏱️  Execution time: {summary1.execution_time_seconds:.2f}s")
        
        # 4. Populate metrics mart
        print("\n4️⃣ Populating metrics mart...")
        
        # Simulate DQ results for metrics population
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
            },
            {
                "rule_id": "payments.fk.account_exists.v01",
                "pass_flag": False,
                "engine": "sql",
                "execution_time_ms": 100,
                "dq_category": "integrity"
            }
        ]
        
        metrics_populator.populate_metrics_from_results(
            dq_results=sample_results,
            dataset="silver.payments",
            execution_date=datetime.now()
        )
        print("✅ Metrics mart populated")
        
        # 5. Generate health summary
        print("\n5️⃣ Generating dataset health summary...")
        
        health_summary = metrics_populator.get_dataset_health_summary(
            dataset="silver.payments",
            days=7
        )
        
        print(f"   📈 Health Score: {health_summary['health_score']}/100")
        print(f"   🏥 Health Status: {health_summary['health_status']}")
        print(f"   📊 Overall Pass Rate: {health_summary['overall_pass_rate']:.2%}")
        print(f"   🔧 Total Rules: {health_summary['total_rules']}")
        
        # 6. Demonstrate Databricks workflow integration (simulation)
        print("\n6️⃣ Databricks Workflow Integration (simulation)...")
        
        # Note: This would require actual Databricks credentials
        if os.getenv('DATABRICKS_TOKEN'):
            try:
                workflow_manager = DatabricksWorkflowManager(
                    workspace_url="https://your-workspace.cloud.databricks.com",
                    token=os.getenv('DATABRICKS_TOKEN'),
                    watermark_manager=watermark_manager
                )
                
                print("✅ Databricks Workflow Manager initialized")
                print("   💡 Use deploy_databricks_workflows.py to create actual jobs")
                
            except Exception as e:
                print(f"   ⚠️  Databricks integration not available: {e}")
        else:
            print("   💡 Set DATABRICKS_TOKEN environment variable to test Databricks integration")
            print("   📝 Use deploy_databricks_workflows.py script to deploy workflows")
        
        # 7. Show watermarks
        print("\n7️⃣ Current watermarks...")
        all_watermarks = watermark_manager.get_all_watermarks()
        for wm in all_watermarks:
            print(f"   📍 {wm.dataset}: {wm.watermark_value} (updated: {wm.updated_ts})")
        
        print("\n🎉 Complete workflow example finished successfully!")
        print("\n📋 Next Steps:")
        print("   1. Deploy to Databricks using deploy_databricks_workflows.py")
        print("   2. Schedule regular DQ runs using the workflow manager")
        print("   3. Set up monitoring and alerting")
        print("   4. Create dashboards using the metrics mart data")
        
    except Exception as e:
        logger.error(f"Example failed: {e}")
        raise
    
    finally:
        spark.stop()


def demonstrate_individual_components():
    """Demonstrate individual components separately."""
    
    print("🔧 Individual Component Demonstration\n")
    
    # Initialize Spark session
    spark = setup_spark_session()
    
    try:
        # 1. Watermark Manager Demo
        print("1️⃣ Watermark Manager Demo...")
        watermark_manager = WatermarkManager(spark=spark, watermark_table_path="/tmp/dq_watermarks")
        
        # Set some watermarks
        watermark_manager.set_watermark("silver.payments", "event_date", "2025-01-15")
        watermark_manager.set_watermark("silver.accounts", "load_date", "2025-01-16")
        
        # Get watermarks
        payments_wm = watermark_manager.get_watermark("silver.payments")
        print(f"   Payments watermark: {payments_wm.watermark_value if payments_wm else 'None'}")
        
        # 2. Metrics Mart Demo
        print("\n2️⃣ Metrics Mart Demo...")
        metrics_populator = MetricsMartPopulator(spark_session=spark)
        
        # Generate health summary
        health = metrics_populator.get_dataset_health_summary("silver.payments")
        print(f"   Health score: {health['health_score']}/100")
        
        # 3. DQ Runner Demo
        print("\n3️⃣ DQ Runner Demo...")
        dq_runner = DQRunner()
        
        rules_file = Path(__file__).parent / "payments_rules.yaml"
        results, summary = dq_runner.run(str(rules_file))
        
        print(f"   DQ Results: {summary.passed_rules}/{summary.total_rules} rules passed")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    print("Choose demonstration:")
    print("1. Complete Workflow Example")
    print("2. Individual Components Demo")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        run_complete_workflow_example()
    elif choice == "2":
        demonstrate_individual_components()
    else:
        print("Invalid choice. Running complete workflow example by default.")
        run_complete_workflow_example()
