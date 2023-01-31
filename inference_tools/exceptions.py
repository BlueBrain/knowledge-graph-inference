from enum import Enum


class InferenceToolsException(Exception):
    """Generic exception."""
    pass


class InferenceToolsWarning(UserWarning):
    """Generic warning."""
    pass


class SimilaritySearchException(InferenceToolsException):
    """Exception in similarity search."""
    pass


class ObjectType(Enum):
    QUERY_PIPE = "query pipe"
    PARAMETER = "parameter"
    RULE = "rule"
    QUERY = "query"
    PREMISE = "premise"


class IncompleteObjectException(InferenceToolsException):
    def __init__(self, attribute, object_type: ObjectType, name=""):
        self.message = f'The {object_type.value} {name} has been ' \
                       f'created with missing mandatory information: {attribute}'

        super().__init__(self.message)


class InvalidValueException(InferenceToolsException):
    def __init__(self, attribute, value, rest=""):
        self.message = f'The {attribute} value \"{value}\" is invalid {rest}'

        super().__init__(self.message)


class UnsupportedParameterTypeException(InferenceToolsException):
    def __init__(self, parameter_type):
        self.message = f"Missing implementation for parameter type {parameter_type.value}"

        super().__init__(self.message)


class InvalidParameterTypeException(InvalidValueException):
    def __init__(self, parameter_type, query_type):

        super().__init__(attribute="parameter type", value=parameter_type.value,
                         rest=f"in a query of type {query_type.value}")
