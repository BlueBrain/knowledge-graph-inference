"""
Describes the states a premise can have after being ran
"""

from enum import Enum


class PremiseExecution(Enum):
    FAIL = "fail"
    SUCCESS = "success"
    MISSING_PARAMETER = "missing_parameter"
