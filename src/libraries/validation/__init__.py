"""
Validation Package

Contains validation logic for DQ rules, schemas, and compliance checking.
Used primarily in CI/CD processes for rule validation before deployment.
"""

from .compliance_checker import ComplianceChecker, check_compliance

__all__ = ["ComplianceChecker", "check_compliance"]
