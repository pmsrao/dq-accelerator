"""
Example usage of the WatermarkManager for incremental data processing.

This example demonstrates how to use the WatermarkManager to track
high water marks for incremental data quality processing.
"""

from datetime import datetime
from pyspark.sql import SparkSession
from src.utils.watermark_manager import WatermarkManager, WatermarkRecord


def main():
    """Example usage of WatermarkManager."""
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("WatermarkManagerExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # Initialize WatermarkManager
    watermark_table_path = "/tmp/dq_watermarks"
    watermark_manager = WatermarkManager(
        spark=spark,
        watermark_table_path=watermark_table_path
    )
    
    print("=== WatermarkManager Example ===\n")
    
    # Example 1: Set initial watermark for a dataset
    print("1. Setting initial watermark for silver.payments...")
    watermark_manager.set_watermark(
        dataset="silver.payments",
        watermark_column="event_date",
        watermark_value="2025-01-15",
        dq_run_completed_ts=datetime(2025, 1, 15, 10, 30, 0)
    )
    
    # Example 2: Set watermark for another dataset
    print("2. Setting watermark for bronze.customers...")
    watermark_manager.set_watermark(
        dataset="bronze.customers",
        watermark_column="load_date",
        watermark_value="2025-01-14",
        dq_run_completed_ts=datetime(2025, 1, 14, 15, 45, 0)
    )
    
    # Example 3: Get watermark for a specific dataset
    print("3. Getting watermark for silver.payments...")
    watermark = watermark_manager.get_watermark("silver.payments")
    if watermark:
        print(f"   Dataset: {watermark.dataset}")
        print(f"   Watermark Column: {watermark.watermark_column}")
        print(f"   Watermark Value: {watermark.watermark_value}")
        print(f"   DQ Run Completed: {watermark.dq_run_completed_ts}")
        print(f"   Updated: {watermark.updated_ts}")
    else:
        print("   No watermark found")
    
    # Example 4: Update watermark after successful processing
    print("\n4. Updating watermark after processing new data...")
    watermark_manager.set_watermark(
        dataset="silver.payments",
        watermark_column="event_date",
        watermark_value="2025-01-16",
        dq_run_completed_ts=datetime(2025, 1, 16, 9, 15, 0)
    )
    
    # Example 5: Get all watermarks
    print("\n5. Getting all watermarks...")
    all_watermarks = watermark_manager.get_all_watermarks()
    for wm in all_watermarks:
        print(f"   {wm.dataset}: {wm.watermark_value} (updated: {wm.updated_ts})")
    
    # Example 6: Get table information
    print("\n6. Getting watermark table information...")
    table_info = watermark_manager.get_watermark_table_info()
    print(f"   Table Path: {table_info['table_path']}")
    print(f"   Total Records: {table_info['total_records']}")
    print(f"   Datasets: {table_info['datasets']}")
    
    # Example 7: Simulate incremental processing workflow
    print("\n7. Simulating incremental processing workflow...")
    
    # Get current watermark
    current_watermark = watermark_manager.get_watermark("silver.payments")
    if current_watermark:
        print(f"   Current watermark: {current_watermark.last_success_value}")
        
        # Simulate processing new partitions
        new_partitions = ["2025-01-17", "2025-01-18", "2025-01-19"]
        print(f"   Processing new partitions: {new_partitions}")
        
        # Update watermark after successful processing
        latest_partition = max(new_partitions)
        watermark_manager.set_watermark(
            dataset="silver.payments",
            partition_column="event_date",
            last_success_value=latest_partition,
            last_success_ts=datetime.utcnow()
        )
        
        print(f"   Updated watermark to: {latest_partition}")
    
    # Example 8: Delete watermark (cleanup)
    print("\n8. Deleting watermark for bronze.customers...")
    watermark_manager.delete_watermark("bronze.customers")
    
    # Verify deletion
    deleted_watermark = watermark_manager.get_watermark("bronze.customers")
    if deleted_watermark is None:
        print("   Watermark successfully deleted")
    else:
        print("   Error: Watermark still exists")
    
    print("\n=== Example completed ===")
    
    # Clean up
    spark.stop()


def incremental_processing_example():
    """
    Example of how to use WatermarkManager in an incremental processing workflow.
    """
    
    spark = SparkSession.builder \
        .appName("IncrementalProcessingExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    watermark_manager = WatermarkManager(
        spark=spark,
        watermark_table_path="/tmp/dq_watermarks"
    )
    
    dataset = "silver.payments"
    watermark_column = "event_date"
    
    # Step 1: Get current watermark
    watermark = watermark_manager.get_watermark(dataset)
    last_processed_value = watermark.watermark_value if watermark else None
    
    print(f"Last processed watermark: {last_processed_value}")
    
    # Step 2: Identify new partitions to process
    # In real scenario, you would query the source table to find new partitions
    if last_processed_value:
        # Simulate finding new partitions
        new_partitions = ["2025-01-20", "2025-01-21", "2025-01-22"]
        print(f"New partitions to process: {new_partitions}")
        
        # Step 3: Process each partition
        for partition_value in new_partitions:
            print(f"Processing partition: {partition_value}")
            
            # Simulate DQ processing
            # In real scenario, you would run your DQ rules here
            success = True  # Simulate successful processing
            
            if success:
                # Step 4: Update watermark after successful processing
                watermark_manager.set_watermark(
                    dataset=dataset,
                    watermark_column=watermark_column,
                    watermark_value=partition_value,
                    dq_run_completed_ts=datetime.utcnow()
                )
                print(f"Successfully processed and updated watermark: {partition_value}")
            else:
                print(f"Failed to process partition: {partition_value}")
                break  # Stop processing on failure
    else:
        print("No previous watermark found. Processing all available partitions.")
        # Handle initial processing case
    
    spark.stop()


if __name__ == "__main__":
    main()
    print("\n" + "="*50 + "\n")
    incremental_processing_example()
