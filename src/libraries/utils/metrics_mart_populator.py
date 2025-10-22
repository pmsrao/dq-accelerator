"""
Metrics Mart Populator

This module provides functionality to populate the metrics mart with
aggregated data quality results and performance metrics.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, isnan, isnull, count, sum, avg, max, min,
    date_format, current_timestamp, coalesce, round
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, 
                             DoubleType, TimestampType, BooleanType

from .watermark_manager import WatermarkManager


class MetricsMartPopulator:
    """
    Populates the metrics mart with aggregated DQ results and performance metrics.
    
    Provides functionality to aggregate DQ execution results, calculate trends,
    and populate the metrics mart for dashboard and reporting purposes.
    """
    
    def __init__(
        self,
        spark_session: SparkSession,
        metrics_mart_path: str = "/tmp/dq_metrics_mart",
        watermark_manager: Optional[WatermarkManager] = None
    ):
        """
        Initialize the Metrics Mart Populator.
        
        Args:
            spark_session: Spark session for data processing
            metrics_mart_path: Path to the metrics mart table
            watermark_manager: Optional watermark manager for incremental processing
        """
        self.spark = spark_session
        self.metrics_mart_path = metrics_mart_path
        self.watermark_manager = watermark_manager
        self.logger = logging.getLogger(__name__)
        
        # Ensure metrics mart table exists
        self._ensure_metrics_mart_table()
    
    def _ensure_metrics_mart_table(self):
        """Ensure the metrics mart table exists with proper schema."""
        try:
            # Check if table exists
            try:
                self.spark.table("dq_metrics.metrics_mart")
                self.logger.info("Metrics mart table already exists")
                return
            except:
                pass
            
            # Create table if it doesn't exist
            self.logger.info("Creating metrics mart table")
            
            # Read the DDL file and execute it
            ddl_path = "src/sql/dq_metrics_mart.sql"
            try:
                with open(ddl_path, 'r') as f:
                    ddl_sql = f.read()
                
                # Execute DDL
                self.spark.sql(ddl_sql)
                self.logger.info("Metrics mart table created successfully")
                
            except FileNotFoundError:
                self.logger.warning(f"DDL file not found: {ddl_path}, creating table with default schema")
                self._create_default_metrics_mart_table()
                
        except Exception as e:
            self.logger.error(f"Failed to ensure metrics mart table: {e}")
            raise
    
    def _create_default_metrics_mart_table(self):
        """Create metrics mart table with default schema."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS dq_metrics.metrics_mart (
            metric_date DATE NOT NULL,
            dataset STRING NOT NULL,
            rule_id STRING NOT NULL,
            engine STRING NOT NULL,
            dq_category STRING,
            pass_count INTEGER NOT NULL,
            fail_count INTEGER NOT NULL,
            total_executions INTEGER NOT NULL,
            pass_rate DOUBLE NOT NULL,
            avg_execution_time_ms DOUBLE,
            min_execution_time_ms INTEGER,
            max_execution_time_ms INTEGER,
            first_execution_ts TIMESTAMP,
            last_execution_ts TIMESTAMP,
            created_ts TIMESTAMP NOT NULL,
            updated_ts TIMESTAMP NOT NULL
        ) USING DELTA
        PARTITIONED BY (metric_date, dataset)
        COMMENT 'Aggregated data quality metrics for dashboard and reporting'
        """
        
        self.spark.sql(create_table_sql)
        self.logger.info("Default metrics mart table created")
    
    def populate_metrics_from_results(
        self,
        dq_results: List[Dict[str, Any]],
        dataset: str,
        execution_date: Optional[datetime] = None
    ) -> None:
        """
        Populate metrics mart from DQ execution results.
        
        Args:
            dq_results: List of DQ execution results
            dataset: Dataset name
            execution_date: Date of execution (defaults to current date)
        """
        if not dq_results:
            self.logger.warning("No DQ results provided for metrics population")
            return
        
        if execution_date is None:
            execution_date = datetime.now()
        
        try:
            self.logger.info(f"Populating metrics for dataset: {dataset}, date: {execution_date.date()}")
            
            # Convert results to DataFrame
            results_df = self.spark.createDataFrame(dq_results)
            
            # Aggregate metrics by rule
            metrics_df = self._aggregate_metrics(results_df, dataset, execution_date)
            
            # Write to metrics mart
            self._write_metrics_to_mart(metrics_df, execution_date)
            
            self.logger.info(f"Successfully populated metrics for {len(dq_results)} results")
            
        except Exception as e:
            self.logger.error(f"Failed to populate metrics: {e}")
            raise
    
    def _aggregate_metrics(
        self,
        results_df: DataFrame,
        dataset: str,
        execution_date: datetime
    ) -> DataFrame:
        """Aggregate DQ results into metrics."""
        
        # Add execution timestamp if not present
        if "execution_ts" not in [field.name for field in results_df.schema.fields]:
            results_df = results_df.withColumn("execution_ts", current_timestamp())
        
        # Aggregate by rule_id and engine
        aggregated = results_df.groupBy("rule_id", "engine", "dq_category") \
            .agg(
                sum(when(col("pass_flag"), 1).otherwise(0)).alias("pass_count"),
                sum(when(col("pass_flag"), 0).otherwise(1)).alias("fail_count"),
                count("*").alias("total_executions"),
                avg("execution_time_ms").alias("avg_execution_time_ms"),
                min("execution_time_ms").alias("min_execution_time_ms"),
                max("execution_time_ms").alias("max_execution_time_ms"),
                min("execution_ts").alias("first_execution_ts"),
                max("execution_ts").alias("last_execution_ts")
            ) \
            .withColumn("pass_rate", 
                       round(col("pass_count") / col("total_executions"), 4)) \
            .withColumn("dataset", lit(dataset)) \
            .withColumn("metric_date", lit(execution_date.date())) \
            .withColumn("created_ts", current_timestamp()) \
            .withColumn("updated_ts", current_timestamp())
        
        return aggregated
    
    def _write_metrics_to_mart(self, metrics_df: DataFrame, execution_date: datetime):
        """Write aggregated metrics to the metrics mart."""
        
        # Write to metrics mart with merge logic
        metrics_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("dq_metrics.metrics_mart")
        
        self.logger.info(f"Metrics written to mart for date: {execution_date.date()}")
    
    def populate_daily_metrics(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> None:
        """
        Populate daily aggregated metrics from DQ results table.
        
        Args:
            start_date: Start date for metrics population (defaults to yesterday)
            end_date: End date for metrics population (defaults to yesterday)
        """
        if start_date is None:
            start_date = datetime.now() - timedelta(days=1)
        if end_date is None:
            end_date = datetime.now() - timedelta(days=1)
        
        try:
            self.logger.info(f"Populating daily metrics from {start_date.date()} to {end_date.date()}")
            
            # Query DQ results for the date range
            results_sql = f"""
            SELECT 
                rule_id,
                engine,
                dq_category,
                pass_flag,
                execution_time_ms,
                execution_ts,
                dataset
            FROM dq_results.dq_results
            WHERE DATE(execution_ts) BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            """
            
            results_df = self.spark.sql(results_sql)
            
            if results_df.count() == 0:
                self.logger.warning(f"No DQ results found for date range {start_date.date()} to {end_date.date()}")
                return
            
            # Group by dataset and date for processing
            datasets = [row.dataset for row in results_df.select("dataset").distinct().collect()]
            
            for dataset in datasets:
                dataset_results = results_df.filter(col("dataset") == dataset)
                
                # Process each date separately
                dates = [row.execution_date for row in 
                        dataset_results.select(date_format(col("execution_ts"), "yyyy-MM-dd").alias("execution_date"))
                        .distinct().collect()]
                
                for date_str in dates:
                    date_results = dataset_results.filter(
                        date_format(col("execution_ts"), "yyyy-MM-dd") == date_str
                    )
                    
                    # Convert to list of dictionaries
                    results_list = [row.asDict() for row in date_results.collect()]
                    
                    # Populate metrics
                    execution_date = datetime.strptime(date_str, "%Y-%m-%d")
                    self.populate_metrics_from_results(results_list, dataset, execution_date)
            
            self.logger.info("Daily metrics population completed")
            
        except Exception as e:
            self.logger.error(f"Failed to populate daily metrics: {e}")
            raise
    
    def calculate_trends(
        self,
        dataset: str,
        rule_id: Optional[str] = None,
        days: int = 30
    ) -> DataFrame:
        """
        Calculate trend metrics for a dataset or specific rule.
        
        Args:
            dataset: Dataset name
            rule_id: Optional specific rule ID
            days: Number of days to analyze
            
        Returns:
            DataFrame with trend metrics
        """
        try:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)
            
            # Build query
            where_clause = f"dataset = '{dataset}' AND metric_date BETWEEN '{start_date}' AND '{end_date}'"
            if rule_id:
                where_clause += f" AND rule_id = '{rule_id}'"
            
            trends_sql = f"""
            SELECT 
                rule_id,
                engine,
                dq_category,
                AVG(pass_rate) as avg_pass_rate,
                MIN(pass_rate) as min_pass_rate,
                MAX(pass_rate) as max_pass_rate,
                STDDEV(pass_rate) as pass_rate_stddev,
                AVG(avg_execution_time_ms) as avg_execution_time,
                COUNT(DISTINCT metric_date) as days_with_data,
                COUNT(*) as total_metric_records
            FROM dq_metrics.metrics_mart
            WHERE {where_clause}
            GROUP BY rule_id, engine, dq_category
            ORDER BY rule_id, engine
            """
            
            trends_df = self.spark.sql(trends_sql)
            self.logger.info(f"Calculated trends for {dataset} over {days} days")
            
            return trends_df
            
        except Exception as e:
            self.logger.error(f"Failed to calculate trends: {e}")
            raise
    
    def get_dataset_health_summary(
        self,
        dataset: str,
        days: int = 7
    ) -> Dict[str, Any]:
        """
        Get a health summary for a dataset.
        
        Args:
            dataset: Dataset name
            days: Number of days to analyze
            
        Returns:
            Dictionary with health summary metrics
        """
        try:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)
            
            summary_sql = f"""
            SELECT 
                COUNT(DISTINCT rule_id) as total_rules,
                COUNT(DISTINCT engine) as total_engines,
                AVG(pass_rate) as overall_pass_rate,
                MIN(pass_rate) as worst_pass_rate,
                MAX(pass_rate) as best_pass_rate,
                AVG(avg_execution_time_ms) as avg_execution_time,
                COUNT(DISTINCT metric_date) as days_analyzed,
                SUM(total_executions) as total_executions
            FROM dq_metrics.metrics_mart
            WHERE dataset = '{dataset}' 
            AND metric_date BETWEEN '{start_date}' AND '{end_date}'
            """
            
            summary_df = self.spark.sql(summary_sql)
            summary_row = summary_df.collect()[0]
            
            # Calculate health score (0-100)
            pass_rate = summary_row.overall_pass_rate or 0
            health_score = int(pass_rate * 100)
            
            # Determine health status
            if health_score >= 95:
                health_status = "EXCELLENT"
            elif health_score >= 90:
                health_status = "GOOD"
            elif health_score >= 80:
                health_status = "FAIR"
            elif health_score >= 70:
                health_status = "POOR"
            else:
                health_status = "CRITICAL"
            
            summary = {
                "dataset": dataset,
                "analysis_period_days": days,
                "health_score": health_score,
                "health_status": health_status,
                "total_rules": summary_row.total_rules or 0,
                "total_engines": summary_row.total_engines or 0,
                "overall_pass_rate": round(pass_rate, 4),
                "worst_pass_rate": round(summary_row.worst_pass_rate or 0, 4),
                "best_pass_rate": round(summary_row.best_pass_rate or 0, 4),
                "avg_execution_time_ms": round(summary_row.avg_execution_time or 0, 2),
                "days_analyzed": summary_row.days_analyzed or 0,
                "total_executions": summary_row.total_executions or 0,
                "generated_at": datetime.now().isoformat()
            }
            
            self.logger.info(f"Generated health summary for {dataset}: {health_status} ({health_score}/100)")
            return summary
            
        except Exception as e:
            self.logger.error(f"Failed to generate health summary: {e}")
            raise
    
    def cleanup_old_metrics(self, retention_days: int = 90) -> None:
        """
        Clean up old metrics data.
        
        Args:
            retention_days: Number of days to retain metrics
        """
        try:
            cutoff_date = (datetime.now() - timedelta(days=retention_days)).date()
            
            cleanup_sql = f"""
            DELETE FROM dq_metrics.metrics_mart
            WHERE metric_date < '{cutoff_date}'
            """
            
            result = self.spark.sql(cleanup_sql)
            self.logger.info(f"Cleaned up metrics older than {cutoff_date}")
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup old metrics: {e}")
            raise
