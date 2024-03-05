

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
