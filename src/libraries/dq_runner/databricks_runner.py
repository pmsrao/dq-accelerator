"""
Databricks DQ Runner - Simplified for Databricks Environment

This module provides a simplified DQ runner specifically designed for Databricks environments.
It assumes 'spark' is available globally and focuses on Databricks-specific features.
"""

import logging
import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field

from ..validation.compliance_checker import check_compliance
from .engines.soda_engine import SodaEngine
from .engines.sql_engine import SqlEngine
from ..utils.watermark_manager import WatermarkManager


class DQResult(BaseModel):
    """Data quality rule execution result."""
    
    rule_id: str
    pass_flag: bool
    engine: str
    execution_time_ms: int
    error_message: Optional[str] = None
    measurements: Optional[Dict[str, Any]] = None


class DQRunSummary(BaseModel):
    """Summary of a DQ run execution."""
    
    run_id: str
    dataset: str
    total_rules: int
    passed_rules: int
    failed_rules: int
    execution_time_seconds: float


class DQRunner:
    """
    Databricks-specific DQ Runner.
    
    Simplified for Databricks environments with 'spark' available globally.
    Focuses on incremental processing with watermark management.
    """
    
    def __init__(
        self,
        watermark_table_name: str = "dq_watermarks",
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the DQ Runner for Databricks environments.
        
        Args:
            watermark_table_name: Name of the watermark table (Unity Catalog managed)
            logger: Optional logger instance
        """
        self.logger = logger or self._setup_logger()
        self.watermark_manager = self._init_watermark_manager(watermark_table_name)
        self.soda_engine: Optional[SodaEngine] = None
        self.sql_engine: Optional[SqlEngine] = None
        
    def _setup_logger(self) -> logging.Logger:
        """Set up logger for the DQ Runner."""
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
    
    def _init_watermark_manager(self, watermark_table_name: str) -> WatermarkManager:
        """Initialize watermark manager with Databricks spark."""
        # In Databricks, 'spark' is available globally
        try:
            import builtins
            spark = builtins.spark
        except:
            # Fallback to globals
            import sys
            frame = sys._getframe(1)
            spark = frame.f_globals['spark']
        
        return WatermarkManager(
            spark=spark,
            watermark_table_name=watermark_table_name
        )
    
    def _get_spark_session(self):
        """Get Spark session from Databricks global scope."""
        try:
            import builtins
            return builtins.spark
        except:
            import sys
            frame = sys._getframe(1)
            return frame.f_globals['spark']
    
    def _initialize_engines(self):
        """Initialize execution engines for Databricks."""
        if not self.soda_engine:
            data_source = {"type": "spark"}
            spark_session = self._get_spark_session()
            self.soda_engine = SodaEngine(data_source, spark_session)
        
        if not self.sql_engine:
            spark_session = self._get_spark_session()
            self.sql_engine = SqlEngine(spark_session)
    
    def load_rules(self, rule_path: str, environment: Optional[str] = None) -> Dict[str, Any]:
        """
        Load DQ rules from file or folder with environment-aware configuration.
        
        Args:
            rule_path: Path to YAML file or folder containing rule files
            environment: Environment name for catalog/schema resolution (dev, staging, prod)
            
        Returns:
            Dictionary containing aggregated rule specification
        """
        try:
            rule_path = Path(rule_path)
            
            if rule_path.is_file():
                # Single file loading
                rules_spec = self._load_single_rule_file(rule_path, environment)
                self.logger.info(f"Loaded {len(rules_spec.get('rules', []))} rules from {rule_path}")
                return rules_spec
                
            elif rule_path.is_dir():
                # Folder-based loading
                rules_spec = self._load_rules_from_folder(rule_path, environment)
                total_rules = len(rules_spec.get('rules', []))
                self.logger.info(f"Loaded {total_rules} rules from {len(list(rule_path.glob('*.yaml')))} files in {rule_path}")
                return rules_spec
            else:
                raise FileNotFoundError(f"Rule path not found: {rule_path}")
                
        except Exception as e:
            self.logger.error(f"Failed to load rules from {rule_path}: {e}")
            raise
    
    def _load_single_rule_file(self, rule_file: Path, environment: Optional[str] = None) -> Dict[str, Any]:
        """Load rules from a single YAML file with environment resolution."""
        with open(rule_file, 'r') as f:
            rules_spec = yaml.safe_load(f)
        
        # Resolve environment-aware table names
        if environment:
            rules_spec = self._resolve_environment_config(rules_spec, environment)
        
        return rules_spec
    
    def _load_rules_from_folder(self, rule_folder: Path, environment: Optional[str] = None) -> Dict[str, Any]:
        """Load and aggregate rules from multiple YAML files in a folder."""
        aggregated_rules = {
            'rules': [],
            'metadata': {
                'source_files': [],
                'total_rules': 0,
                'environment': environment
            }
        }
        
        # Find all YAML files in the folder
        yaml_files = list(rule_folder.glob('*.yaml')) + list(rule_folder.glob('*.yml'))
        
        if not yaml_files:
            self.logger.warning(f"No YAML files found in {rule_folder}")
            return aggregated_rules
        
        for yaml_file in sorted(yaml_files):
            try:
                self.logger.debug(f"Loading rules from {yaml_file}")
                file_rules = self._load_single_rule_file(yaml_file, environment)
                
                # Aggregate rules
                if 'rules' in file_rules:
                    aggregated_rules['rules'].extend(file_rules['rules'])
                    aggregated_rules['metadata']['source_files'].append(str(yaml_file))
                
                # Merge metadata (use first file's metadata as base)
                if len(aggregated_rules['rules']) == len(file_rules.get('rules', [])):
                    for key, value in file_rules.items():
                        if key not in ['rules', 'metadata']:
                            aggregated_rules[key] = value
                            
            except Exception as e:
                self.logger.error(f"Failed to load rules from {yaml_file}: {e}")
                continue
        
        aggregated_rules['metadata']['total_rules'] = len(aggregated_rules['rules'])
        return aggregated_rules
    
    def _resolve_environment_config(self, rules_spec: Dict[str, Any], environment: str) -> Dict[str, Any]:
        """Resolve environment-specific configuration for catalog and schema."""
        try:
            # Load environment configuration
            env_config = self._load_environment_config(environment)
            
            # Resolve dataset name with catalog
            if 'dataset' in rules_spec:
                original_dataset = rules_spec['dataset']
                resolved_dataset = self._resolve_table_name(original_dataset, env_config)
                rules_spec['dataset'] = resolved_dataset
                self.logger.info(f"Resolved dataset: {original_dataset} -> {resolved_dataset}")
            
            # Resolve SQL queries with catalog references
            if 'rules' in rules_spec:
                for rule in rules_spec['rules']:
                    if 'sql' in rule:
                        rule['sql'] = self._resolve_sql_with_catalog(rule['sql'], env_config)
            
            return rules_spec
            
        except Exception as e:
            self.logger.warning(f"Failed to resolve environment config for {environment}: {e}")
            return rules_spec
    
    def _load_environment_config(self, environment: str) -> Dict[str, Any]:
        """Load environment-specific configuration."""
        try:
            config_path = f"config/{environment}.yaml"
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    return yaml.safe_load(f)
            else:
                # Return default configuration
                return {
                    'catalog': 'main',
                    'default_schema': 'silver',
                    'table_prefix': ''
                }
        except Exception as e:
            self.logger.warning(f"Failed to load environment config: {e}")
            return {'catalog': 'main', 'default_schema': 'silver'}
    
    def _resolve_table_name(self, dataset: str, env_config: Dict[str, Any]) -> str:
        """Resolve table name with catalog and environment configuration."""
        catalog = env_config.get('catalog', 'main')
        
        # If dataset already has catalog, use as-is
        if '.' in dataset and len(dataset.split('.')) >= 3:
            return dataset
        
        # If dataset is schema.table, add catalog
        if '.' in dataset:
            schema, table = dataset.split('.', 1)
            return f"{catalog}.{schema}.{table}"
        
        # If dataset is just table name, add catalog and default schema
        default_schema = env_config.get('default_schema', 'silver')
        return f"{catalog}.{default_schema}.{dataset}"
    
    def _resolve_sql_with_catalog(self, sql: str, env_config: Dict[str, Any]) -> str:
        """Resolve SQL queries with catalog references."""
        catalog = env_config.get('catalog', 'main')
        
        # Replace schema.table references with catalog.schema.table
        import re
        pattern = r'\b(\w+)\.(\w+)\b'
        
        def replace_table_ref(match):
            schema, table = match.groups()
            return f"{catalog}.{schema}.{table}"
        
        return re.sub(pattern, replace_table_ref, sql)
    
    def run_incremental(
        self,
        rule_path: str,
        dataset: str,
        watermark_column: str,
        environment: Optional[str] = None
    ) -> DQRunSummary:
        """
        Run DQ rules with incremental processing using watermarks.
        
        Args:
            rule_path: Path to YAML file or folder containing rule files
            dataset: Dataset name to process
            watermark_column: Column used for watermarking
            environment: Environment name for catalog/schema resolution
            
        Returns:
            DQRunSummary with execution results
        """
        self.logger.info(f"Starting incremental DQ run for dataset: {dataset}")
        
        # Load and validate rules
        rules_spec = self.load_rules(rule_path, environment)
        check_compliance(rules_spec)
        
        # Get current watermark
        watermark = self.watermark_manager.get_watermark(dataset)
        last_processed_value = watermark.watermark_value if watermark else None
        
        self.logger.info(f"Last processed watermark: {last_processed_value}")
        
        # Get new watermark values to process
        new_watermark_values = self._get_new_watermark_values(dataset, watermark_column, last_processed_value)
        
        if not new_watermark_values:
            self.logger.info("No new watermark values to process")
            return DQRunSummary(
                run_id=str(uuid.uuid4()),
                dataset=dataset,
                total_rules=0,
                passed_rules=0,
                failed_rules=0,
                execution_time_seconds=0.0
            )
        
        self.logger.info(f"Processing {len(new_watermark_values)} new watermark values: {new_watermark_values}")
        
        # Process each watermark value
        all_results = []
        total_start_time = time.time()
        
        for watermark_value in new_watermark_values:
            try:
                self.logger.info(f"Processing watermark value: {watermark_value}")
                
                # Run DQ rules for this watermark value
                watermark_results = self._run_watermark_value(rules_spec, dataset, watermark_value)
                all_results.extend(watermark_results)
                
                # Update watermark after successful processing
                self.watermark_manager.set_watermark(
                    dataset=dataset,
                    watermark_column=watermark_column,
                    watermark_value=watermark_value,
                    dq_run_completed_ts=datetime.now()
                )
                
                self.logger.info(f"Successfully processed watermark value: {watermark_value}")
                
            except Exception as e:
                self.logger.error(f"Failed to process watermark value {watermark_value}: {e}")
                # Continue with next watermark value instead of failing entire run
                continue
        
        total_execution_time = time.time() - total_start_time
        
        # Create summary
        summary = self._create_run_summary(all_results, dataset, total_execution_time)
        
        self.logger.info(f"Incremental DQ run completed. Processed {len(new_watermark_values)} watermark values")
        return summary
    
    def _get_new_watermark_values(
        self,
        dataset: str,
        watermark_column: str,
        last_processed_value: Optional[str]
    ) -> List[str]:
        """
        Get list of new watermark values to process using Databricks SQL.
        
        Args:
            dataset: Dataset name
            watermark_column: Watermark column name
            last_processed_value: Last processed watermark value
            
        Returns:
            List of watermark values to process
        """
        try:
            spark = self._get_spark_session()
            
            # Query to find new watermark values
            if last_processed_value:
                sql = f"""
                SELECT DISTINCT {watermark_column}
                FROM {dataset}
                WHERE {watermark_column} > '{last_processed_value}'
                ORDER BY {watermark_column}
                """
            else:
                sql = f"""
                SELECT DISTINCT {watermark_column}
                FROM {dataset}
                ORDER BY {watermark_column}
                """
            
            # Execute query to get watermark values
            result_df = spark.sql(sql)
            watermark_values = [row[watermark_column] for row in result_df.collect()]
            
            return watermark_values
            
        except Exception as e:
            self.logger.error(f"Failed to get new watermark values: {e}")
            return []
    
    def _run_watermark_value(
        self,
        rules_spec: Dict[str, Any],
        dataset: str,
        watermark_value: str
    ) -> List[DQResult]:
        """
        Run DQ rules for a specific watermark value.
        
        Args:
            rules_spec: Rule specification dictionary
            dataset: Dataset name
            watermark_value: Watermark value to process
            
        Returns:
            List of DQ results for the watermark value
        """
        # Initialize engines if not already done
        self._initialize_engines()
        
        rules = rules_spec.get("rules", [])
        all_results = []
        
        # Group rules by engine
        soda_rules = [r for r in rules if r.get("engine") == "soda"]
        sql_rules = [r for r in rules if r.get("engine") == "sql"]
        
        # Execute Soda rules
        if soda_rules and self.soda_engine:
            try:
                soda_results, _, _ = self.soda_engine.run(dataset, soda_rules, watermark_value)
                for result in soda_results:
                    all_results.append(DQResult(
                        rule_id=result["rule_id"],
                        pass_flag=result["pass_flag"],
                        engine="soda",
                        execution_time_ms=result.get("execution_time_ms", 0),
                        measurements=result.get("measured_value")
                    ))
            except Exception as e:
                self.logger.error(f"Soda engine execution failed: {e}")
                # Add error results for soda rules
                for rule in soda_rules:
                    all_results.append(DQResult(
                        rule_id=rule["id"],
                        pass_flag=False,
                        engine="soda",
                        execution_time_ms=0,
                        error_message=str(e)
                    ))
        
        # Execute SQL rules
        if sql_rules and self.sql_engine:
            try:
                sql_results, _ = self.sql_engine.run(sql_rules, watermark_value)
                for result in sql_results:
                    all_results.append(DQResult(
                        rule_id=result["rule_id"],
                        pass_flag=result["pass_flag"],
                        engine="sql",
                        execution_time_ms=result.get("execution_time_ms", 0),
                        measurements=result.get("measured_value")
                    ))
            except Exception as e:
                self.logger.error(f"SQL engine execution failed: {e}")
                # Add error results for sql rules
                for rule in sql_rules:
                    all_results.append(DQResult(
                        rule_id=rule["id"],
                        pass_flag=False,
                        engine="sql",
                        execution_time_ms=0,
                        error_message=str(e)
                    ))
        
        return all_results
    
    def _create_run_summary(
        self,
        results: List[DQResult],
        dataset: str,
        execution_time: float
    ) -> DQRunSummary:
        """Create run summary from results."""
        passed_count = sum(1 for r in results if r.pass_flag)
        failed_count = len(results) - passed_count
        
        return DQRunSummary(
            run_id=str(uuid.uuid4()),
            dataset=dataset,
            total_rules=len(results),
            passed_rules=passed_count,
            failed_rules=failed_count,
            execution_time_seconds=execution_time
        )
