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

"""
Exceptions
"""

from inference_tools.type import ObjectTypeStr


class InferenceToolsException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class IncompleteObjectException(InferenceToolsException):
    def __init__(self, attribute, object_type: ObjectTypeStr, name=""):
        self.name = name
        super().__init__(f'The {object_type.value} {name} has been '
                         f'created with missing mandatory information: {attribute}')


class InvalidValueException(InferenceToolsException):
    def __init__(self, attribute, value, rest=""):
        super().__init__(f'The {attribute} value \"{value}\" is invalid {rest}')


class UnsupportedTypeException(InferenceToolsException):
    def __init__(self, parameter_type, type_type):
        super().__init__(f"Missing implementation for {type_type} {parameter_type.value}")


class MissingPremiseParameterValue(InferenceToolsException):
    def __init__(self, param_name):
        super().__init__(f"Premise cannot be ran because parameter {param_name}"
                         f" has not been provided")


class FailedQueryException(InferenceToolsException):
    def __init__(self, description):
        super().__init__(f"The following query has returned no results: {description}")


class SimilaritySearchException(InferenceToolsException):
    """Exception in similarity search."""
