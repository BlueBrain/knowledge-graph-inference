"""
Exceptions
"""
from inference_tools.type import ObjectType


class InferenceToolsException(Exception):
    """Generic exception."""


class InferenceToolsWarning(UserWarning):
    """Generic warning."""


class SimilaritySearchException(InferenceToolsException):
    """Exception in similarity search."""


class IncompleteObjectException(InferenceToolsException):
    def __init__(self, attribute, object_type: ObjectType, name=""):
        self.message = f'The {object_type.value} {name} has been ' \
                       f'created with missing mandatory information: {attribute}'
        self.name = name
        super().__init__(self.message)


class InvalidValueException(InferenceToolsException):
    def __init__(self, attribute, value, rest=""):
        self.message = f'The {attribute} value \"{value}\" is invalid {rest}'

        super().__init__(self.message)


class UnsupportedTypeException(InferenceToolsException):
    def __init__(self, parameter_type, type_type):
        self.message = f"Missing implementation for {type_type} {parameter_type.value}"

        super().__init__(self.message)


class InvalidParameterTypeException(InvalidValueException):
    def __init__(self, parameter_type, query_type):

        super().__init__(attribute="parameter type", value=parameter_type.value,
                         rest=f"in a query of type {query_type.value}")


class MissingPremiseParameterValue(InferenceToolsException):
    def __init__(self, param_name):
        self.message = f"Premise cannot be ran because parameter {param_name} has not been provided"
        super().__init__(self.message)


class InvalidParameterSpecificationException(InferenceToolsException):
    def __init(self, message):
        self.message = message
        super().__init__(message)
