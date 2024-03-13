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

class MalformedRuleException(Exception):
    """Exception for rules that are malformed."""
    def __init__(self, message):
        self.message = message
        super().__init__(message)


class InvalidParameterSpecificationException(MalformedRuleException):
    ...


class InvalidParameterTypeException(MalformedRuleException):
    def __init__(self, parameter_type, query_type):
        super().__init__(
            f'The parameter type {parameter_type.value}\" is invalid'
            f'in a query of type {query_type.value}')


class MalformedSimilaritySearchQueryException(MalformedRuleException):
    ...
