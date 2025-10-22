#!/usr/bin/env python3
"""
Databricks Workflow Deployment Script

This script deploys and manages Databricks workflows for data quality processing.
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

import yaml

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from integrations.databricks_workflow import DatabricksWorkflowManager, create_dq_workflow_from_config


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Set up logging for the deployment script."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def load_config(config_file: str) -> Dict:
    """Load configuration from YAML file."""
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)


def deploy_workflows(
    config_file: str,
    environment: str = "dev",
    workflow_names: Optional[List[str]] = None
) -> Dict[str, List[str]]:
    """
    Deploy Databricks workflows based on configuration.
    
    Args:
        config_file: Path to configuration file
        environment: Environment to deploy to
        workflow_names: Specific workflows to deploy (None for all)
        
    Returns:
        Dictionary mapping workflow names to job IDs
    """
    logger = logging.getLogger(__name__)
    
    # Load configuration
    config = load_config(config_file)
    
    # Get environment-specific config
    env_config = config.get('environments', {}).get(environment, {})
    
    # Merge base config with environment config
    merged_config = {**config, **env_config}
    
    # Initialize Databricks client
    try:
        workflow_manager = create_dq_workflow_from_config(config_file)
    except Exception as e:
        logger.error(f"Failed to initialize Databricks client: {e}")
        raise
    
    # Get workflow definitions
    workflows = merged_config.get('workflows', {})
    
    if workflow_names:
        workflows = {k: v for k, v in workflows.items() if k in workflow_names}
    
    deployed_jobs = {}
    
    for workflow_name, workflow_config in workflows.items():
        logger.info(f"Deploying workflow: {workflow_name}")
        
        try:
            # Get workflow settings
            schedule = workflow_config.get('schedule')
            incremental = workflow_config.get('incremental', True)
            datasets = workflow_config.get('datasets', [])
            dataset_configs = workflow_config.get('dataset_configs', {})
            
            # Get cluster configuration
            cluster_config = merged_config.get('databricks', {}).get('default_cluster', {})
            
            # Get email notifications
            email_notifications = merged_config.get('databricks', {}).get('email_notifications', {})
            
            # Get job settings
            job_settings = merged_config.get('databricks', {}).get('job_settings', {})
            
            # Deploy jobs for each dataset
            job_ids = []
            for dataset in datasets:
                dataset_config = dataset_configs.get(dataset, {})
                
                job_name = f"{workflow_name}_{dataset.replace('.', '_')}"
                rules_file = dataset_config.get('rules_file', f"src/schemas/examples/{dataset.split('.')[-1]}_rules.yaml")
                watermark_column = dataset_config.get('watermark_column', 'event_date')
                
                job_id = workflow_manager.create_dq_job(
                    job_name=job_name,
                    rules_file_path=rules_file,
                    dataset=dataset,
                    schedule=schedule,
                    cluster_config=cluster_config,
                    email_notifications=email_notifications,
                    max_concurrent_runs=job_settings.get('max_concurrent_runs', 1),
                    timeout_seconds=job_settings.get('timeout_seconds', 3600),
                    incremental=incremental,
                    watermark_column=watermark_column if incremental else None
                )
                
                job_ids.append(job_id)
                logger.info(f"Created job for {dataset}: {job_id}")
            
            deployed_jobs[workflow_name] = job_ids
            
        except Exception as e:
            logger.error(f"Failed to deploy workflow {workflow_name}: {e}")
            raise
    
    return deployed_jobs


def list_workflows(config_file: str) -> None:
    """List all deployed workflows."""
    logger = logging.getLogger(__name__)
    
    try:
        workflow_manager = create_dq_workflow_from_config(config_file)
        jobs = workflow_manager.list_jobs()
        
        print("\nDeployed Databricks Jobs:")
        print("-" * 80)
        for job in jobs:
            print(f"Job ID: {job['job_id']}")
            print(f"Name: {job['name']}")
            print(f"Created: {job['created_time']}")
            print(f"Creator: {job['creator_user_name']}")
            print(f"Max Concurrent Runs: {job['max_concurrent_runs']}")
            print(f"Timeout: {job['timeout_seconds']}s")
            print("-" * 80)
    
    except Exception as e:
        logger.error(f"Failed to list workflows: {e}")
        raise


def run_workflow(
    config_file: str,
    workflow_name: str,
    dataset: Optional[str] = None
) -> None:
    """Run a specific workflow immediately."""
    logger = logging.getLogger(__name__)
    
    try:
        workflow_manager = create_dq_workflow_from_config(config_file)
        
        # List jobs and find matching ones
        jobs = workflow_manager.list_jobs(name_filter=workflow_name)
        
        if not jobs:
            logger.error(f"No jobs found for workflow: {workflow_name}")
            return
        
        for job in jobs:
            if dataset and dataset not in job['name']:
                continue
            
            logger.info(f"Running job: {job['name']} ({job['job_id']})")
            run_id = workflow_manager.run_job_now(job['job_id'])
            
            logger.info(f"Job triggered, run ID: {run_id}")
            
            # Wait for completion
            try:
                status = workflow_manager.wait_for_job_completion(run_id, timeout_minutes=30)
                logger.info(f"Job completed with state: {status['state']}")
            except TimeoutError:
                logger.warning(f"Job {run_id} did not complete within timeout")
    
    except Exception as e:
        logger.error(f"Failed to run workflow: {e}")
        raise


def delete_workflows(
    config_file: str,
    workflow_names: List[str]
) -> None:
    """Delete specified workflows."""
    logger = logging.getLogger(__name__)
    
    try:
        workflow_manager = create_dq_workflow_from_config(config_file)
        
        for workflow_name in workflow_names:
            jobs = workflow_manager.list_jobs(name_filter=workflow_name)
            
            for job in jobs:
                logger.info(f"Deleting job: {job['name']} ({job['job_id']})")
                workflow_manager.delete_job(job['job_id'])
        
        logger.info("Workflow deletion completed")
    
    except Exception as e:
        logger.error(f"Failed to delete workflows: {e}")
        raise


def main():
    """Main entry point for the deployment script."""
    parser = argparse.ArgumentParser(description="Deploy and manage Databricks workflows")
    parser.add_argument("--config", required=True, help="Configuration file path")
    parser.add_argument("--environment", default="dev", choices=["dev", "staging", "prod"],
                       help="Environment to deploy to")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Deploy command
    deploy_parser = subparsers.add_parser("deploy", help="Deploy workflows")
    deploy_parser.add_argument("--workflows", nargs="+", help="Specific workflows to deploy")
    
    # List command
    subparsers.add_parser("list", help="List deployed workflows")
    
    # Run command
    run_parser = subparsers.add_parser("run", help="Run a workflow immediately")
    run_parser.add_argument("--workflow", required=True, help="Workflow name to run")
    run_parser.add_argument("--dataset", help="Specific dataset to run")
    
    # Delete command
    delete_parser = subparsers.add_parser("delete", help="Delete workflows")
    delete_parser.add_argument("--workflows", nargs="+", required=True, help="Workflows to delete")
    
    args = parser.parse_args()
    
    # Set up logging
    logger = setup_logging(args.log_level)
    
    # Check for required environment variables
    if not os.getenv('DATABRICKS_TOKEN'):
        logger.error("DATABRICKS_TOKEN environment variable is required")
        sys.exit(1)
    
    try:
        if args.command == "deploy":
            deployed_jobs = deploy_workflows(
                config_file=args.config,
                environment=args.environment,
                workflow_names=args.workflows
            )
            
            print("\nDeployment Summary:")
            for workflow_name, job_ids in deployed_jobs.items():
                print(f"{workflow_name}: {len(job_ids)} jobs deployed")
                for job_id in job_ids:
                    print(f"  - {job_id}")
        
        elif args.command == "list":
            list_workflows(args.config)
        
        elif args.command == "run":
            run_workflow(
                config_file=args.config,
                workflow_name=args.workflow,
                dataset=args.dataset
            )
        
        elif args.command == "delete":
            delete_workflows(
                config_file=args.config,
                workflow_names=args.workflows
            )
        
        else:
            parser.print_help()
    
    except Exception as e:
        logger.error(f"Command failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
