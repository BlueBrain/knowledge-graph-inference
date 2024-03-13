# This file is part of knowledge-graph-inference.
# Copyright 2024 Blue Brain Project / EPFL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
