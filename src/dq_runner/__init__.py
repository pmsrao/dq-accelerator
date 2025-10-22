"""
DQ Runner Package

Core execution engine for data quality rules with support for multiple engines
(Soda Core and SQL) and incremental processing.
"""

from .runner import DQRunner
from .compliance_checker import check_compliance

__all__ = ["DQRunner", "check_compliance"]
