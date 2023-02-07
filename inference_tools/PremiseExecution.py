from enum import Enum


class PremiseExecution(Enum):
    FAIL = "fail"
    SUCCESS = "success"
    MISSING_PARAMETER = "missing_parameter"
    ERROR = "error"
