"""
Environment-Aware Data Quality Rules Example

This example demonstrates:
1. File-based rule loading
2. Folder-based rule loading
3. Environment-aware catalog resolution
4. Both single file and multiple file scenarios
"""

import os
from pathlib import Path
from src.libraries.dq_runner.databricks_runner import DQRunner

def demonstrate_file_based_loading():
    """Demonstrate loading rules from a single file."""
    print("=== File-Based Rule Loading ===")
    
    # Initialize DQ Runner
    dq_runner = DQRunner()
    
    # Load rules from single file
    rules_spec = dq_runner.load_rules(
        rule_path="src/schemas/examples/payments_basic_rules.yaml",
        environment="dev"
    )
    
    print(f"Loaded {len(rules_spec.get('rules', []))} rules from single file")
    print(f"Dataset: {rules_spec.get('dataset')}")
    print(f"Environment: {rules_spec.get('metadata', {}).get('environment')}")
    
    # Show how dataset name is resolved
    print(f"Original dataset in YAML: silver.payments")
    print(f"Resolved dataset: {rules_spec.get('dataset')}")
    print()

def demonstrate_folder_based_loading():
    """Demonstrate loading rules from a folder."""
    print("=== Folder-Based Rule Loading ===")
    
    # Initialize DQ Runner
    dq_runner = DQRunner()
    
    # Create a temporary folder with multiple rule files
    rules_folder = "src/schemas/examples"
    
    # Load rules from folder
    rules_spec = dq_runner.load_rules(
        rule_path=rules_folder,
        environment="prod"
    )
    
    print(f"Loaded {len(rules_spec.get('rules', []))} rules from folder")
    print(f"Source files: {rules_spec.get('metadata', {}).get('source_files', [])}")
    print(f"Total rules: {rules_spec.get('metadata', {}).get('total_rules', 0)}")
    print(f"Environment: {rules_spec.get('metadata', {}).get('environment')}")
    print()

def demonstrate_environment_resolution():
    """Demonstrate how table names are resolved for different environments."""
    print("=== Environment-Aware Table Resolution ===")
    
    dq_runner = DQRunner()
    
    # Test different environments
    environments = ["dev", "staging", "prod"]
    
    for env in environments:
        print(f"\n--- {env.upper()} Environment ---")
        
        # Load rules with environment
        rules_spec = dq_runner.load_rules(
            rule_path="src/schemas/examples/payments_basic_rules.yaml",
            environment=env
        )
        
        print(f"Environment: {env}")
        print(f"Resolved dataset: {rules_spec.get('dataset')}")
        
        # Show SQL resolution
        for rule in rules_spec.get('rules', []):
            if 'sql' in rule:
                print(f"Original SQL: {rule.get('sql', '')[:100]}...")
                break
    print()

def demonstrate_catalog_resolution():
    """Demonstrate catalog resolution in SQL queries."""
    print("=== Catalog Resolution in SQL ===")
    
    dq_runner = DQRunner()
    
    # Load rules with environment
    rules_spec = dq_runner.load_rules(
        rule_path="src/schemas/examples/payments_advanced_rules.yaml",
        environment="prod"
    )
    
    print("SQL queries with catalog resolution:")
    for rule in rules_spec.get('rules', []):
        if 'sql' in rule:
            print(f"Rule: {rule.get('id')}")
            print(f"SQL: {rule.get('sql')}")
            print()
    print()

def demonstrate_rule_aggregation():
    """Demonstrate how rules are aggregated from multiple files."""
    print("=== Rule Aggregation from Multiple Files ===")
    
    dq_runner = DQRunner()
    
    # Load rules from folder (multiple files)
    rules_spec = dq_runner.load_rules(
        rule_path="src/schemas/examples",
        environment="staging"
    )
    
    print(f"Total rules loaded: {len(rules_spec.get('rules', []))}")
    print(f"Source files: {len(rules_spec.get('metadata', {}).get('source_files', []))}")
    
    # Group rules by category
    categories = {}
    for rule in rules_spec.get('rules', []):
        category = rule.get('dq_category', 'unknown')
        if category not in categories:
            categories[category] = 0
        categories[category] += 1
    
    print("\nRules by category:")
    for category, count in categories.items():
        print(f"  {category}: {count} rules")
    print()

if __name__ == "__main__":
    print("Environment-Aware Data Quality Rules Example")
    print("=" * 50)
    
    # Demonstrate different approaches
    demonstrate_file_based_loading()
    demonstrate_folder_based_loading()
    demonstrate_environment_resolution()
    demonstrate_catalog_resolution()
    demonstrate_rule_aggregation()
    
    print("Example completed successfully!")
