"""
Databricks Workflow Integration

This module provides integration with Databricks Workflows for automated
data quality rule execution with job scheduling and monitoring.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import (
        JobSettings, 
        NotebookTask, 
        PythonWheelTask,
        Task,
        JobCluster,
        NewCluster,
        JobEmailNotifications,
        JobRunAs,
        RunParameters
    )
    DATABRICKS_SDK_AVAILABLE = True
except ImportError:
    DATABRICKS_SDK_AVAILABLE = False
    WorkspaceClient = None

from ..utils.watermark_manager import WatermarkManager


class DatabricksWorkflowManager:
    """
    Manages Databricks Workflows for data quality rule execution.
    
    Provides functionality to create, schedule, and monitor Databricks jobs
    for automated DQ rule execution with incremental processing support.
    """
    
    def __init__(
        self,
        workspace_url: str,
        token: str,
        watermark_manager: Optional[WatermarkManager] = None
    ):
        """
        Initialize Databricks Workflow Manager.
        
        Args:
            workspace_url: Databricks workspace URL
            token: Databricks access token
            watermark_manager: Optional watermark manager for incremental processing
        """
        if not DATABRICKS_SDK_AVAILABLE:
            raise ImportError(
                "Databricks SDK is not available. Install with: pip install databricks-sdk"
            )
        
        self.workspace_url = workspace_url
        self.token = token
        self.watermark_manager = watermark_manager
        self.logger = logging.getLogger(__name__)
        
        # Initialize Databricks client
        self.client = WorkspaceClient(
            host=workspace_url,
            token=token
        )
    
    def create_dq_job(
        self,
        job_name: str,
        rules_file_path: str,
        dataset: str,
        schedule: Optional[str] = None,
        cluster_config: Optional[Dict[str, Any]] = None,
        email_notifications: Optional[Dict[str, List[str]]] = None,
        max_concurrent_runs: int = 1,
        timeout_seconds: int = 3600,
        incremental: bool = False,
        watermark_column: Optional[str] = None
    ) -> str:
        """
        Create a Databricks job for DQ rule execution.
        
        Args:
            job_name: Name of the job
            rules_file_path: Path to the DQ rules YAML file
            dataset: Dataset name to process
            schedule: Cron expression for scheduling (optional)
            cluster_config: Cluster configuration (optional)
            email_notifications: Email notification settings (optional)
            max_concurrent_runs: Maximum number of concurrent runs
            timeout_seconds: Job timeout in seconds
            incremental: Whether to use incremental processing
            watermark_column: Watermark column for incremental processing
            
        Returns:
            Job ID
        """
        try:
            # Create job tasks
            tasks = self._create_dq_tasks(
                rules_file_path=rules_file_path,
                dataset=dataset,
                incremental=incremental,
                watermark_column=watermark_column,
                cluster_config=cluster_config
            )
            
            # Create job settings
            job_settings = JobSettings(
                name=job_name,
                tasks=tasks,
                max_concurrent_runs=max_concurrent_runs,
                timeout_seconds=timeout_seconds,
                email_notifications=self._create_email_notifications(email_notifications),
                run_as=JobRunAs(service_principal_name=None)  # Use current user
            )
            
            # Add schedule if provided
            if schedule:
                job_settings.schedule = {"quartz_cron_expression": schedule, "timezone_id": "UTC"}
            
            # Create the job
            job = self.client.jobs.create(**job_settings.dict())
            job_id = job.job_id
            
            self.logger.info(f"Created Databricks job '{job_name}' with ID: {job_id}")
            return job_id
            
        except Exception as e:
            self.logger.error(f"Failed to create Databricks job: {e}")
            raise
    
    def _create_dq_tasks(
        self,
        rules_file_path: str,
        dataset: str,
        incremental: bool,
        watermark_column: Optional[str],
        cluster_config: Optional[Dict[str, Any]]
    ) -> List[Task]:
        """Create tasks for DQ job execution."""
        tasks = []
        
        # Create cluster configuration
        cluster = self._create_cluster_config(cluster_config)
        
        # Main DQ execution task
        if incremental and watermark_column:
            # Incremental processing task
            task = Task(
                task_key="dq_incremental_execution",
                description=f"Incremental DQ execution for {dataset}",
                job_cluster_key="dq_cluster",
                python_wheel_task=PythonWheelTask(
                    package_name="dq-accelerator",
                    entry_point="dq_runner.databricks_job_entries:run_incremental_job",
                    parameters=[
                        "--rules-file", rules_file_path,
                        "--dataset", dataset,
                        "--watermark-column", watermark_column
                    ]
                )
            )
        else:
            # Full processing task
            task = Task(
                task_key="dq_full_execution",
                description=f"Full DQ execution for {dataset}",
                job_cluster_key="dq_cluster",
                python_wheel_task=PythonWheelTask(
                    package_name="dq-accelerator",
                    entry_point="dq_runner.databricks_job_entries:run_full_job",
                    parameters=[
                        "--rules-file", rules_file_path,
                        "--dataset", dataset
                    ]
                )
            )
        
        tasks.append(task)
        
        # Add watermark cleanup task if using incremental processing
        if incremental and self.watermark_manager:
            cleanup_task = Task(
                task_key="watermark_cleanup",
                description="Cleanup old watermarks",
                job_cluster_key="dq_cluster",
                depends_on=[{"task_key": "dq_incremental_execution"}],
                python_wheel_task=PythonWheelTask(
                    package_name="dq-accelerator",
                    entry_point="utils.watermark_manager:cleanup_old_watermarks",
                    parameters=["--dataset", dataset, "--retention-days", "30"]
                )
            )
            tasks.append(cleanup_task)
        
        return tasks
    
    def _create_cluster_config(self, cluster_config: Optional[Dict[str, Any]]) -> JobCluster:
        """Create cluster configuration for the job."""
        default_config = {
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "driver_node_type_id": "i3.xlarge",
            "num_workers": 2,
            "spark_conf": {
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.databricks.delta.retentionDurationCheck.enabled": "false"
            },
            "aws_attributes": {
                "availability": "SPOT_WITH_FALLBACK",
                "zone_id": "us-west-2a"
            }
        }
        
        # Merge with provided config
        if cluster_config:
            default_config.update(cluster_config)
        
        return JobCluster(
            job_cluster_key="dq_cluster",
            new_cluster=NewCluster(**default_config)
        )
    
    def _create_email_notifications(
        self, 
        email_config: Optional[Dict[str, List[str]]]
    ) -> Optional[JobEmailNotifications]:
        """Create email notification configuration."""
        if not email_config:
            return None
        
        return JobEmailNotifications(
            on_start=email_config.get("on_start", []),
            on_success=email_config.get("on_success", []),
            on_failure=email_config.get("on_failure", [])
        )
    
    def run_job_now(self, job_id: str, parameters: Optional[Dict[str, Any]] = None) -> str:
        """
        Trigger a job run immediately.
        
        Args:
            job_id: Job ID to run
            parameters: Optional job parameters
            
        Returns:
            Run ID
        """
        try:
            run_params = None
            if parameters:
                run_params = RunParameters(parameters)
            
            run = self.client.jobs.run_now(job_id=job_id, run_params=run_params)
            run_id = run.run_id
            
            self.logger.info(f"Triggered job {job_id}, run ID: {run_id}")
            return run_id
            
        except Exception as e:
            self.logger.error(f"Failed to trigger job {job_id}: {e}")
            raise
    
    def get_job_status(self, run_id: str) -> Dict[str, Any]:
        """
        Get the status of a job run.
        
        Args:
            run_id: Run ID to check
            
        Returns:
            Job run status information
        """
        try:
            run = self.client.jobs.get_run(run_id=run_id)
            
            return {
                "run_id": run_id,
                "state": run.state.life_cycle_state if run.state else "UNKNOWN",
                "result_state": run.state.result_state if run.state else None,
                "start_time": run.start_time,
                "end_time": run.end_time,
                "execution_duration": run.execution_duration,
                "setup_duration": run.setup_duration,
                "cleanup_duration": run.cleanup_duration,
                "run_page_url": run.run_page_url
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get job status for run {run_id}: {e}")
            raise
    
    def wait_for_job_completion(
        self, 
        run_id: str, 
        timeout_minutes: int = 60,
        poll_interval_seconds: int = 30
    ) -> Dict[str, Any]:
        """
        Wait for a job run to complete.
        
        Args:
            run_id: Run ID to wait for
            timeout_minutes: Maximum time to wait in minutes
            poll_interval_seconds: Polling interval in seconds
            
        Returns:
            Final job status
        """
        start_time = time.time()
        timeout_seconds = timeout_minutes * 60
        
        while time.time() - start_time < timeout_seconds:
            status = self.get_job_status(run_id)
            state = status["state"]
            
            if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                self.logger.info(f"Job {run_id} completed with state: {state}")
                return status
            
            self.logger.info(f"Job {run_id} still running, state: {state}")
            time.sleep(poll_interval_seconds)
        
        raise TimeoutError(f"Job {run_id} did not complete within {timeout_minutes} minutes")
    
    def list_jobs(self, name_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all jobs, optionally filtered by name.
        
        Args:
            name_filter: Optional name filter
            
        Returns:
            List of job information
        """
        try:
            jobs = self.client.jobs.list()
            
            job_list = []
            for job in jobs:
                if name_filter and name_filter.lower() not in job.settings.name.lower():
                    continue
                
                job_list.append({
                    "job_id": job.job_id,
                    "name": job.settings.name,
                    "created_time": job.created_time,
                    "creator_user_name": job.creator_user_name,
                    "run_as_user_name": job.settings.run_as.user_name if job.settings.run_as else None,
                    "max_concurrent_runs": job.settings.max_concurrent_runs,
                    "timeout_seconds": job.settings.timeout_seconds
                })
            
            return job_list
            
        except Exception as e:
            self.logger.error(f"Failed to list jobs: {e}")
            raise
    
    def delete_job(self, job_id: str) -> bool:
        """
        Delete a Databricks job.
        
        Args:
            job_id: Job ID to delete
            
        Returns:
            True if successful
        """
        try:
            self.client.jobs.delete(job_id=job_id)
            self.logger.info(f"Deleted job {job_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to delete job {job_id}: {e}")
            raise
    
    def create_dq_workflow_template(
        self,
        template_name: str,
        rules_file_path: str,
        datasets: List[str],
        schedule: str = "0 0 * * *",  # Daily at midnight
        cluster_config: Optional[Dict[str, Any]] = None,
        email_notifications: Optional[Dict[str, List[str]]] = None,
        incremental: bool = True
    ) -> List[str]:
        """
        Create a workflow template with multiple datasets.
        
        Args:
            template_name: Base name for the workflow
            rules_file_path: Path to the DQ rules YAML file
            datasets: List of datasets to process
            schedule: Cron expression for scheduling
            cluster_config: Cluster configuration
            email_notifications: Email notification settings
            incremental: Whether to use incremental processing
            
        Returns:
            List of created job IDs
        """
        job_ids = []
        
        for dataset in datasets:
            job_name = f"{template_name}_{dataset.replace('.', '_')}"
            
            # Extract partition column from dataset name or use default
            partition_column = "event_date"  # Default partition column
            
            job_id = self.create_dq_job(
                job_name=job_name,
                rules_file_path=rules_file_path,
                dataset=dataset,
                schedule=schedule,
                cluster_config=cluster_config,
                email_notifications=email_notifications,
                incremental=incremental,
                partition_column=partition_column
            )
            
            job_ids.append(job_id)
            self.logger.info(f"Created job for dataset {dataset}: {job_id}")
        
        return job_ids


def create_dq_workflow_from_config(config_file: str) -> DatabricksWorkflowManager:
    """
    Create a Databricks Workflow Manager from configuration file.
    
    Args:
        config_file: Path to configuration file
        
    Returns:
        Configured DatabricksWorkflowManager instance
    """
    import yaml
    
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    # Extract Databricks configuration
    databricks_config = config.get('databricks', {})
    
    return DatabricksWorkflowManager(
        workspace_url=databricks_config['workspace_url'],
        token=databricks_config['token']
    )
