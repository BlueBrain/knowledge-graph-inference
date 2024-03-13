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

from typing import Any, Optional, Dict

from inference_tools.exceptions.exceptions import IncompleteObjectException, InferenceToolsException

from inference_tools.type import ObjectTypeStr, ParameterType, parameter_list_types
from inference_tools.helper_functions import _get_type, _enforce_list


class ParameterSpecification:
    name: str
    description: Optional[str]
    optional: bool = False
    default: Optional[Any] = None
    type: ParameterType
    values: Optional[Dict[str, Any]]

    def __init__(self, obj):
        self.name = obj["name"]
        self.description = obj.get("description", "")
        self.optional = obj.get("optional", False)
        self.default = obj.get("default", None)
        self.type = _get_type(obj, ObjectTypeStr.PARAMETER, ParameterType)
        self.values = obj.get("values", None)  # For parameter type with choice enabled

    def __eq__(self, other):
        return self.name == self.name and self.description == self.description and \
            self.optional == self.optional and self.default == self.default and \
            self.type == self.type and self.values == self.values

    def __repr__(self):
        name_str = f"Name: {self.name}"
        desc_str = f"Description: {self.description}"
        optional_str = f"Optional: {self.optional}"
        default_str = f"Default: {self.default}"
        type_str = f"Type: {self.type.value}"
        values_str = f"Values: {self.values}"
        return "\n".join([name_str, desc_str, optional_str, default_str, type_str, values_str])

    def to_dict(self) -> Dict:
        """
        Returns the dict version of the parameter specification, in the format initially parsed
        @return: the original dict format of a ParameterSpecification
        @rtype: Dict
        """
        return {
            "name": self.name,
            "description": self.description,
            "optional": self.optional,
            "default": self.default,
            "type": self.type.value,
            "values": self.values
        }

    def get_value(self, parameter_values: Dict[str, str]) -> Any:
        """
        From the parameter values specified by the user, retrieves the value associated with
        this input parameter specification by its name
        @param parameter_values: the parameter values specified by the user
        @type parameter_values: Dict[str, str]
        @return: the parameter value corresponding to this parameter specification
        @rtype: Any
        """
        if self.name in parameter_values and parameter_values[self.name] is not None:
            v = parameter_values[self.name]

            if len(v) == 0:
                return []

            if self.values is None:
                return v

            # restricted set of valid values
            selected_value_keys = _enforce_list(v)
            valid_values = list(self.values.keys())

            if any(vi not in valid_values for vi in selected_value_keys):
                raise InferenceToolsException(
                    f"Invalid value for parameter {self.name}, valid values are {valid_values}"
                )

            selected_values = [self.values[v] for v in selected_value_keys]
            return selected_values if len(selected_values) > 1 else selected_values[0]

        if self.default is not None:
            return self.default
        if self.optional:
            return None if self.type not in parameter_list_types else []
        raise IncompleteObjectException(
            name=self.name, attribute="value", object_type=ObjectTypeStr.PARAMETER
        )
