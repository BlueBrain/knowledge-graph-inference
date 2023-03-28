from typing import Any, Optional, Dict

from inference_tools.exceptions import IncompleteObjectException, InferenceToolsException

from inference_tools.type import ObjectTypeStr, ParameterType
from inference_tools.helper_functions import _get_type


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

    def to_dict(self):
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
        if self.name in parameter_values and parameter_values[self.name]:
            if self.values is not None:
                if parameter_values[self.name] not in self.values.keys():
                    raise InferenceToolsException(f"Invalid value for parameter {self.name}")

                return self.values[parameter_values[self.name]]
            else:
                return parameter_values[self.name]

        if self.default is not None:
            return self.default
        if self.optional:
            return None
        raise IncompleteObjectException(name=self.name, attribute="value",
                                        object_type=ObjectTypeStr.PARAMETER)
