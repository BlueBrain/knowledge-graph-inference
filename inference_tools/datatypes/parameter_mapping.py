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

class ParameterMapping:
    """
    When obtaining results from the execution of a query, the query results will be consumed.
    A parameter mapping can be used in other to specify what information to extract within each
    result, through a path to follow inside a dictionary. The information found at that path will be
    set as the value tied to the key defined by the parameter name of the parameter mapping.
    """
    parameter_name: str
    path: str

    def __init__(self, obj):
        self.parameter_name = obj.get("parameterName", None)
        self.path = obj.get("path", None)

    def __repr__(self):
        return f"Parameter Name: {self.parameter_name} ; Path: {self.path}"
