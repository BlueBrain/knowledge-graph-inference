from typing import List

from inference_tools.exceptions.exceptions import InferenceToolsException
from inference_tools.premise_execution import PremiseExecution


class PremiseException(InferenceToolsException):
    ...


class FailedPremiseException(PremiseException):
    def __init__(self, description):
        super().__init__(f"The following premise query has returned no results: {description}")


class IrrelevantPremiseParametersException(PremiseException):
    def __init__(self):

        super().__init__("The premise(s) failed because the provided parameters are "
                         "irrelevant to the ones required by the premises")


class UnsupportedPremiseCaseException(PremiseException):
    def __init__(self, flags: List[PremiseExecution]):
        super().__init__("The status of premise checking is unclear with the following premise "
                         f"execution flags: {','.join([flag.value for flag in flags])}")


class MalformedPremiseException(PremiseException):
    ...
