"""
DQ Runner Package

Core execution engine for data quality rules with support for multiple engines
(Soda Core and SQL) and incremental processing. Optimized for Databricks environments.
"""

from .databricks_runner import DQRunner

__all__ = ["DQRunner"]
