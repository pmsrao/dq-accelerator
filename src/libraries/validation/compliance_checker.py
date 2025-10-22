"""
Compliance Checker for Data Quality Rules.

This module provides validation and compliance checking for DQ rule specifications,
including JSON schema validation and business rule validation.
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Set

import yaml
from jsonschema import ValidationError, validate


class DQComplianceError(Exception):
    """Custom exception for DQ compliance errors."""
    pass


class ComplianceChecker:
    """
    Validates data quality rule specifications for compliance.
    
    Performs both JSON schema validation and business rule validation
    to ensure rules are properly defined and compatible.
    """
    
    # Valid DQ categories
    VALID_CATEGORIES: Set[str] = {
        "accuracy", "completeness", "consistency", "conformity",
        "uniqueness", "freshness", "validity", "integrity", "anomaly"
    }
    
    # Category to check type mappings
    CATEGORY_CHECK_TYPE_MAPPINGS: Dict[str, Set[str]] = {
        "freshness": {"freshness_lag", "freshness_check"},
        "uniqueness": {"uniqueness", "primary_key", "unique_key"},
        "completeness": {"not_null", "not_empty", "completeness"},
        "validity": {"value_range", "regex_match", "enum_check", "data_type"},
        "consistency": {"cross_table", "referential_integrity", "format_consistency"},
        "accuracy": {"accuracy_check", "cross_validation"},
        "conformity": {"format_check", "standard_compliance"},
        "integrity": {"foreign_key", "referential_integrity", "fk_exists"},
        "anomaly": {"statistical_anomaly", "outlier_detection", "pattern_anomaly"}
    }
    
    def __init__(self, schema_path: str):
        """
        Initialize the compliance checker.
        
        Args:
            schema_path: Path to the JSON schema file
        """
        self.schema_path = Path(schema_path)
        self.schema = self._load_schema()
        
    def _load_schema(self) -> Dict[str, Any]:
        """
        Load JSON schema from file.
        
        Returns:
            JSON schema dictionary
            
        Raises:
            FileNotFoundError: If schema file doesn't exist
            json.JSONDecodeError: If schema is invalid JSON
        """
        try:
            with open(self.schema_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Schema file not found: {self.schema_path}")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Invalid JSON schema: {e}")
            
    def validate_schema(self, rules_spec: Dict[str, Any]) -> List[str]:
        """
        Validate rules specification against JSON schema.
        
        Args:
            rules_spec: Rules specification dictionary
            
        Returns:
            List of validation error messages
        """
        errors = []
        try:
            validate(instance=rules_spec, schema=self.schema)
        except ValidationError as e:
            errors.append(f"JSON Schema validation failed: {e.message}")
            if e.absolute_path:
                errors.append(f"  Path: {' -> '.join(str(p) for p in e.absolute_path)}")
        return errors
        
    def validate_business_rules(self, rules_spec: Dict[str, Any]) -> List[str]:
        """
        Validate business rules and compatibility constraints.
        
        Args:
            rules_spec: Rules specification dictionary
            
        Returns:
            List of business rule validation error messages
        """
        errors = []
        rules = rules_spec.get("rules", [])
        
        for rule in rules:
            rule_id = rule.get("id", "unknown")
            errors.extend(self._validate_single_rule(rule, rule_id))
            
        return errors
        
    def _validate_single_rule(self, rule: Dict[str, Any], rule_id: str) -> List[str]:
        """
        Validate a single rule for business logic compliance.
        
        Args:
            rule: Single rule dictionary
            rule_id: Rule identifier for error messages
            
        Returns:
            List of validation error messages for this rule
        """
        errors = []
        
        # Validate DQ category
        category = rule.get("dq_category")
        if category and category not in self.VALID_CATEGORIES:
            errors.append(f"Rule {rule_id}: Invalid DQ category '{category}'")
            
        # Validate category-check type compatibility
        check_type = rule.get("dq_check_type")
        if category and check_type:
            valid_check_types = self.CATEGORY_CHECK_TYPE_MAPPINGS.get(category, set())
            if valid_check_types and check_type not in valid_check_types:
                errors.append(
                    f"Rule {rule_id}: Category '{category}' expects check types: "
                    f"{', '.join(valid_check_types)}, got '{check_type}'"
                )
                
        # Validate engine-specific requirements
        engine = rule.get("engine")
        if engine == "sql" and not rule.get("sql"):
            errors.append(f"Rule {rule_id}: SQL engine requires 'sql' field")
        elif engine == "soda" and not rule.get("params"):
            # Soda rules typically need params, but this might be flexible
            pass
            
        # Validate rule ID format
        if not self._is_valid_rule_id(rule_id):
            errors.append(
                f"Rule {rule_id}: Rule ID should follow pattern: "
                "domain.table.column.check_type.version (e.g., payments.amount.non_negative.v01)"
            )
            
        # Validate required fields based on check type
        errors.extend(self._validate_check_type_requirements(rule, rule_id))
        
        return errors
        
    def _is_valid_rule_id(self, rule_id: str) -> bool:
        """
        Validate rule ID format.
        
        Args:
            rule_id: Rule identifier string
            
        Returns:
            True if rule ID format is valid
        """
        # Pattern: domain.table.column.check_type.version
        pattern = r'^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*\.v\d{2}$'
        return bool(re.match(pattern, rule_id))
        
    def _validate_check_type_requirements(self, rule: Dict[str, Any], rule_id: str) -> List[str]:
        """
        Validate requirements specific to check types.
        
        Args:
            rule: Rule dictionary
            rule_id: Rule identifier
            
        Returns:
            List of validation error messages
        """
        errors = []
        check_type = rule.get("dq_check_type")
        
        if check_type == "value_range":
            params = rule.get("params", {})
            if "operator" not in params or "operand" not in params:
                errors.append(f"Rule {rule_id}: value_range check requires 'operator' and 'operand' in params")
                
        elif check_type == "uniqueness":
            if not rule.get("keys") and not rule.get("columns"):
                errors.append(f"Rule {rule_id}: uniqueness check requires 'keys' or 'columns' field")
                
        elif check_type == "not_null":
            if not rule.get("columns"):
                errors.append(f"Rule {rule_id}: not_null check requires 'columns' field")
                
        elif check_type == "foreign_key" or check_type == "fk_exists":
            if not rule.get("sql"):
                errors.append(f"Rule {rule_id}: {check_type} check requires 'sql' field")
                
        return errors
        
    def check_compliance(self, rule_yaml_path: str) -> List[str]:
        """
        Perform complete compliance check on a rule file.
        
        Args:
            rule_yaml_path: Path to the YAML rule file
            
        Returns:
            List of all validation error messages
        """
        try:
            with open(rule_yaml_path, 'r') as f:
                rules_spec = yaml.safe_load(f)
        except FileNotFoundError:
            return [f"Rule file not found: {rule_yaml_path}"]
        except yaml.YAMLError as e:
            return [f"Invalid YAML file {rule_yaml_path}: {e}"]
            
        errors = []
        
        # Schema validation
        errors.extend(self.validate_schema(rules_spec))
        
        # Business rule validation
        errors.extend(self.validate_business_rules(rules_spec))
        
        return errors


def check_compliance(schema_path: str, rule_yaml_path: str) -> List[str]:
    """
    Convenience function for compliance checking.
    
    Args:
        schema_path: Path to JSON schema file
        rule_yaml_path: Path to YAML rule file
        
    Returns:
        List of validation error messages
    """
    checker = ComplianceChecker(schema_path)
    return checker.check_compliance(rule_yaml_path)
