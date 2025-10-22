"""
DQ Runner - Main execution engine for data quality rules.

This module provides the main DQRunner class and command-line interface for executing
data quality rules using multiple engines (Soda Core and SQL).
"""

import argparse
import json
import logging
import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
from pydantic import BaseModel, Field

from .compliance_checker import check_compliance
from .engines.soda_engine import SodaEngine
from .engines.sql_engine import SqlEngine


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
    partition_value: str
    started_ts: str
    ended_ts: str
    total_rules: int
    passed_rules: int
    failed_rules: int
    execution_time_seconds: float


class DQRunner:
    """
    Main data quality rule execution engine.
    
    Supports multiple execution engines (Soda Core, SQL) and provides
    comprehensive error handling, logging, and result aggregation.
    """
    
    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the DQ Runner.
        
        Args:
            config: Configuration dictionary for the runner
            logger: Optional logger instance
        """
        self.config = config or {}
        self.logger = logger or self._setup_logger()
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
        
    def load_rules(self, rule_file_path: str) -> Dict[str, Any]:
        """
        Load DQ rules from YAML file.
        
        Args:
            rule_file_path: Path to the YAML rule file
            
        Returns:
            Dictionary containing rule specification
            
        Raises:
            FileNotFoundError: If rule file doesn't exist
            yaml.YAMLError: If YAML parsing fails
        """
        try:
            with open(rule_file_path, 'r') as f:
                rules = yaml.safe_load(f)
            self.logger.info(f"Loaded rules from {rule_file_path}")
            return rules
        except FileNotFoundError:
            self.logger.error(f"Rule file not found: {rule_file_path}")
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"Failed to parse YAML file {rule_file_path}: {e}")
            raise
            
    def filter_rules_by_engine(self, rules: List[Dict[str, Any]], engine_name: str) -> List[Dict[str, Any]]:
        """
        Filter rules by execution engine.
        
        Args:
            rules: List of rule dictionaries
            engine_name: Name of the engine to filter by
            
        Returns:
            Filtered list of rules for the specified engine
        """
        return [rule for rule in rules if rule.get("engine") == engine_name]
        
    def initialize_engines(self, data_source_config: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize execution engines.
        
        Args:
            data_source_config: Configuration for data source connections
        """
        try:
            data_source = data_source_config or {"type": "spark"}
            self.soda_engine = SodaEngine(data_source=data_source)
            self.sql_engine = SqlEngine(sql_client=None)  # TODO: Implement proper SQL client
            self.logger.info("Successfully initialized execution engines")
        except Exception as e:
            self.logger.error(f"Failed to initialize engines: {e}")
            raise
            
    def execute_rules(
        self,
        rules_spec: Dict[str, Any],
        dataset: Optional[str] = None,
        partition_value: str = "latest"
    ) -> Tuple[List[DQResult], DQRunSummary]:
        """
        Execute data quality rules.
        
        Args:
            rules_spec: Rule specification dictionary
            dataset: Target dataset name
            partition_value: Partition value for execution
            
        Returns:
            Tuple of (results, summary)
        """
        run_id = str(uuid.uuid4())[:12]
        started_time = time.time()
        started_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        
        self.logger.info(f"Starting DQ run {run_id} for dataset {dataset}")
        
        # Get dataset from spec if not provided
        target_dataset = dataset or rules_spec.get("dataset")
        if not target_dataset:
            raise ValueError("Dataset must be provided either as parameter or in rules spec")
            
        # Initialize engines if not already done
        if not self.soda_engine or not self.sql_engine:
            self.initialize_engines()
            
        # Filter rules by engine
        all_rules = rules_spec.get("rules", [])
        soda_rules = self.filter_rules_by_engine(all_rules, "soda")
        sql_rules = self.filter_rules_by_engine(all_rules, "sql")
        
        results: List[DQResult] = []
        
        # Execute Soda rules
        if soda_rules and self.soda_engine:
            try:
                self.logger.info(f"Executing {len(soda_rules)} Soda rules")
                soda_results, soda_yaml, soda_ms = self.soda_engine.run(
                    target_dataset, soda_rules, partition_value
                )
                for result in soda_results:
                    result["engine"] = "soda"
                    results.append(DQResult(**result))
                self.logger.info(f"Soda rules completed in {soda_ms}ms")
            except Exception as e:
                self.logger.error(f"Failed to execute Soda rules: {e}")
                # Add failed results for Soda rules
                for rule in soda_rules:
                    results.append(DQResult(
                        rule_id=rule["id"],
                        pass_flag=False,
                        engine="soda",
                        execution_time_ms=0,
                        error_message=str(e)
                    ))
                    
        # Execute SQL rules
        if sql_rules and self.sql_engine:
            try:
                self.logger.info(f"Executing {len(sql_rules)} SQL rules")
                sql_results, sql_ms = self.sql_engine.run(sql_rules, partition_value)
                for result in sql_results:
                    result["engine"] = "sql"
                    results.append(DQResult(**result))
                self.logger.info(f"SQL rules completed in {sql_ms}ms")
            except Exception as e:
                self.logger.error(f"Failed to execute SQL rules: {e}")
                # Add failed results for SQL rules
                for rule in sql_rules:
                    results.append(DQResult(
                        rule_id=rule["id"],
                        pass_flag=False,
                        engine="sql",
                        execution_time_ms=0,
                        error_message=str(e)
                    ))
                    
        # Calculate summary
        ended_time = time.time()
        ended_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        execution_time = ended_time - started_time
        
        passed_count = sum(1 for r in results if r.pass_flag)
        failed_count = len(results) - passed_count
        
        summary = DQRunSummary(
            run_id=run_id,
            dataset=target_dataset,
            partition_value=partition_value,
            started_ts=started_ts,
            ended_ts=ended_ts,
            total_rules=len(results),
            passed_rules=passed_count,
            failed_rules=failed_count,
            execution_time_seconds=execution_time
        )
        
        self.logger.info(
            f"DQ run {run_id} completed: {passed_count}/{len(results)} rules passed "
            f"in {execution_time:.2f}s"
        )
        
        return results, summary


def main() -> None:
    """Main entry point for command-line execution."""
    parser = argparse.ArgumentParser(description="DQ Runner (Soda + SQL)")
    parser.add_argument("--rule-file", required=True, help="Path to YAML rule file")
    parser.add_argument(
        "--schema-file", 
        default=os.path.join(os.path.dirname(__file__), "..", "schemas", "dq_rule_schema.json"),
        help="Path to JSON schema file"
    )
    parser.add_argument("--dataset", help="Target dataset name")
    parser.add_argument("--partition", help="Partition value (e.g., 2025-10-21)")
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level)
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration if provided
        config = {}
        if args.config:
            with open(args.config, 'r') as f:
                config = yaml.safe_load(f)
                
        # Compliance check
        logger.info("Performing compliance check...")
        errors = check_compliance(args.schema_file, args.rule_file)
        if errors:
            logger.error("Compliance check failed:")
            for error in errors:
                logger.error(f"  - {error}")
            raise SystemExit(1)
        logger.info("Compliance check passed")
        
        # Initialize runner
        runner = DQRunner(config=config, logger=logger)
        
        # Load rules
        rules_spec = runner.load_rules(args.rule_file)
        
        # Execute rules
        results, summary = runner.execute_rules(
            rules_spec=rules_spec,
            dataset=args.dataset,
            partition_value=args.partition or "latest"
        )
        
        # Output results
        output = {
            "run_id": summary.run_id,
            "dataset": summary.dataset,
            "partition_value": summary.partition_value,
            "started_ts": summary.started_ts,
            "ended_ts": summary.ended_ts,
            "execution_time_seconds": summary.execution_time_seconds,
            "summary": {
                "total": summary.total_rules,
                "passed": summary.passed_rules,
                "failed": summary.failed_rules
            },
            "results": [result.dict() for result in results]
        }
        
        print(json.dumps(output, indent=2))
        
        # Exit with error code if any rules failed
        if summary.failed_rules > 0:
            raise SystemExit(1)
            
    except Exception as e:
        logger.error(f"DQ Runner failed: {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
