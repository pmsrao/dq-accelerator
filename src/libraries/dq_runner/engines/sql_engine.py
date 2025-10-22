"""
SQL Engine - Real Spark SQL integration for data quality checks.

This module provides integration with Spark SQL for executing custom SQL-based
data quality rules with proper error handling and result processing.
"""

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, isnan, isnull
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


class SqlEngine:
    """
    Spark SQL integration engine for data quality rule execution.
    
    Provides real integration with Spark SQL for executing custom SQL-based
    data quality checks with comprehensive error handling and result processing.
    """
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """
        Initialize the SQL Engine.
        
        Args:
            spark_session: Spark session for SQL execution
        """
        self.spark_session = spark_session
        self.logger = logging.getLogger(__name__)
        
        if not self.spark_session:
            self.logger.warning("No Spark session provided, SQL execution will fail")
    
    
    def _render_sql_template(self, sql_template: str, partition_value: str, **kwargs) -> str:
        """
        Render SQL template with partition value and other parameters.
        
        Args:
            sql_template: SQL template with placeholders
            partition_value: Partition value to substitute
            **kwargs: Additional parameters for template rendering
            
        Returns:
            Rendered SQL query
        """
        # Replace partition value placeholder
        rendered_sql = sql_template.replace("${PARTITION_VALUE}", partition_value)
        
        # Replace additional parameters
        for key, value in kwargs.items():
            placeholder = f"${{{key.upper()}}}"
            rendered_sql = rendered_sql.replace(placeholder, str(value))
        
        return rendered_sql
    
    def _execute_sql_query(self, sql: str) -> Dict[str, Any]:
        """
        Execute SQL query and return results.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            Query results as dictionary
        """
        
        try:
            self.logger.debug(f"Executing SQL: {sql}")
            
            # Execute SQL query
            result_df = self.spark_session.sql(sql)
            
            # Collect results
            results = result_df.collect()
            
            if not results:
                self.logger.warning("SQL query returned no results")
                return {}
            
            # Convert first row to dictionary
            first_row = results[0]
            result_dict = first_row.asDict()
            
            self.logger.debug(f"SQL query result: {result_dict}")
            return result_dict
            
        except Exception as e:
            self.logger.error(f"SQL query execution failed: {e}")
            raise
    
    def _validate_threshold(self, measured_value: Any, threshold: Dict[str, Any]) -> bool:
        """
        Validate measured value against threshold.
        
        Args:
            measured_value: Value to validate
            threshold: Threshold configuration with operator and operand
            
        Returns:
            True if validation passes, False otherwise
        """
        operator = threshold.get("operator", "=")
        operand = threshold.get("operand", 0)
        
        try:
            if operator == "=":
                return measured_value == operand
            elif operator == "!=":
                return measured_value != operand
            elif operator == ">":
                return measured_value > operand
            elif operator == ">=":
                return measured_value >= operand
            elif operator == "<":
                return measured_value < operand
            elif operator == "<=":
                return measured_value <= operand
            else:
                self.logger.warning(f"Unknown operator: {operator}")
                return False
        except Exception as e:
            self.logger.error(f"Threshold validation failed: {e}")
            return False
    
    def _build_standard_sql_checks(self, rule: Dict[str, Any], dataset: str, partition_value: str) -> str:
        """
        Build standard SQL checks for common DQ rule types.
        
        Args:
            rule: DQ rule configuration
            dataset: Dataset name
            partition_value: Partition value
            
        Returns:
            SQL query for the check
        """
        check_type = rule.get("dq_check_type")
        columns = rule.get("columns", [])
        keys = rule.get("keys", [])
        
        if check_type == "uniqueness" and keys:
            # Uniqueness check
            key_columns = ", ".join(keys) if isinstance(keys, list) else str(keys)
            sql = f"""
            SELECT COUNT(*) as violations
            FROM (
                SELECT {key_columns}, COUNT(*) as cnt
                FROM {dataset}
                WHERE event_date = '{partition_value}'
                GROUP BY {key_columns}
                HAVING COUNT(*) > 1
            ) duplicates
            """
            
        elif check_type == "completeness" and columns:
            # Completeness check
            column = columns[0]
            sql = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT({column}) as non_null_rows,
                (COUNT(*) - COUNT({column})) as null_count,
                ROUND((COUNT(*) - COUNT({column})) * 100.0 / COUNT(*), 2) as null_percent
            FROM {dataset}
            WHERE event_date = '{partition_value}'
            """
            
        elif check_type == "validity" and columns:
            # Validity check (basic null check)
            column = columns[0]
            sql = f"""
            SELECT COUNT(*) as violations
            FROM {dataset}
            WHERE event_date = '{partition_value}'
            AND ({column} IS NULL OR {column} = '')
            """
            
        elif check_type == "value_range" and columns and rule.get("params"):
            # Value range check
            column = columns[0]
            operator = rule["params"].get("operator", ">=")
            operand = rule["params"].get("operand", 0)
            sql = f"""
            SELECT COUNT(*) as violations
            FROM {dataset}
            WHERE event_date = '{partition_value}'
            AND {column} {operator} {operand}
            """
            
        else:
            # Default row count check
            sql = f"""
            SELECT COUNT(*) as row_count
            FROM {dataset}
            WHERE event_date = '{partition_value}'
            """
        
        return sql.strip()
    
    def run(self, rules: List[Dict[str, Any]], partition_value: str) -> Tuple[List[Dict[str, Any]], int]:
        """
        Execute SQL-based DQ rules.
        
        Args:
            rules: List of DQ rules to execute
            partition_value: Partition value for incremental processing
            
        Returns:
            Tuple of (results, execution_time_ms)
        """
        start_time = time.time()
        results = []
        
        try:
            for rule in rules:
                rule_id = rule.get("id", "unknown")
                engine = rule.get("engine", "sql")
                
                # Skip non-SQL rules
                if engine != "sql":
                    continue
                
                try:
                    # Get SQL query
                    if rule.get("sql"):
                        # Custom SQL provided
                        sql = self._render_sql_template(rule["sql"], partition_value)
                    else:
                        # Build standard SQL check
                        dataset = rule.get("dataset", "unknown")
                        sql = self._build_standard_sql_checks(rule, dataset, partition_value)
                    
                    # Execute SQL
                    query_result = self._execute_sql_query(sql)
                    
                    # Determine pass/fail based on threshold
                    threshold = rule.get("threshold", {"operator": "=", "operand": 0})
                    
                    # Extract violation count or measured value
                    violations = query_result.get("violations", 0)
                    measured_value = query_result
                    
                    # Validate against threshold
                    pass_flag = self._validate_threshold(violations, threshold)
                    
                    results.append({
                        "rule_id": rule_id,
                        "pass_flag": pass_flag,
                        "pass_count": 1 if pass_flag else 0,
                        "fail_count": 0 if pass_flag else 1,
                        "measured_value": measured_value,
                        "sql_executed": sql
                    })
                    
                except Exception as e:
                    self.logger.error(f"Failed to execute rule {rule_id}: {e}")
                    results.append({
                        "rule_id": rule_id,
                        "pass_flag": False,
                        "pass_count": 0,
                        "fail_count": 1,
                        "measured_value": {"error": str(e)},
                        "sql_executed": ""
                    })
            
            execution_time = int((time.time() - start_time) * 1000)
            return results, execution_time
            
        except Exception as e:
            execution_time = int((time.time() - start_time) * 1000)
            self.logger.error(f"SQL engine execution failed: {e}")
            
            # Return error results for all rules
            for rule in rules:
                if rule.get("engine") == "sql":
                    results.append({
                        "rule_id": rule.get("id", "unknown"),
                        "pass_flag": False,
                        "pass_count": 0,
                        "fail_count": 1,
                        "measured_value": {"error": str(e)},
                        "sql_executed": ""
                    })
            
            return results, execution_time
    
    def validate_sql_syntax(self, sql: str) -> bool:
        """
        Validate SQL syntax without executing.
        
        Args:
            sql: SQL query to validate
            
        Returns:
            True if syntax is valid, False otherwise
        """
        try:
            
            # Try to create a logical plan without executing
            self.spark_session.sql(f"EXPLAIN {sql}")
            return True
            
        except Exception as e:
            self.logger.error(f"SQL syntax validation failed: {e}")
            return False
    
    def get_table_schema(self, table_name: str) -> Optional[StructType]:
        """
        Get schema information for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Table schema or None if table doesn't exist
        """
        try:
            
            # Get table schema
            df = self.spark_session.table(table_name)
            return df.schema
            
        except Exception as e:
            self.logger.error(f"Failed to get schema for table {table_name}: {e}")
            return None
    
    def check_table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists.
        
        Args:
            table_name: Name of the table
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            
            # Try to access the table
            self.spark_session.table(table_name)
            return True
            
        except Exception:
            return False