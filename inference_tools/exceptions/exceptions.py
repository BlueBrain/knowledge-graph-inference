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

