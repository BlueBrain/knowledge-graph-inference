class InferenceToolsException(Exception):
    """Generic exception."""
    pass


class MissingParameterException(InferenceToolsException):
    """Exception for missing query/premise parameters."""
    pass

class InvalidParameterException(InferenceToolsException):
    """Exception for invalid query/premise parameters."""
    pass

class PremiseException(InferenceToolsException):
    """Exception in premise checking."""
    pass

class PremiseTypeException(InferenceToolsException):
    """Exception in premise checking."""
    pass

class QueryException(InferenceToolsException):
    """Generic exception in querying."""
    pass


class QueryTypeException(InferenceToolsException):
    """Generic exception in querying."""
    pass


class SimilaritySearchException(InferenceToolsException):
    """Exception in similarity search."""
    pass


class InferenceToolsWarning(UserWarning):
    """Generic warning."""
    pass


class SearchWarning(UserWarning):
    """Search warning."""
    pass


class MissingParameterWarning(UserWarning):
    """Missing parameter warning."""
    pass
