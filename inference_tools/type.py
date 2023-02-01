from enum import Enum
from inference_tools.helper_functions import _enforce_unique, _expand_uri
from inference_tools.exceptions import UnsupportedTypeException, InvalidParameterTypeException


class ParameterType(Enum):
    LIST = "list"
    URI_LIST = "uri_list"
    SPARQL_VALUE_LIST = "sparql_value_list"
    SPARQL_VALUE_URI_LIST = "sparql_value_uri_list"
    SPARQL_LIST = "sparql_list"
    URI = "uri"
    STR = "str"
    PATH = "path"
    MULTI_PREDICATE_OBJECT_PAIR = "MultiPredicateObjectPair"


class QueryType(Enum):
    SPARQL_QUERY = "SparqlQuery"
    ELASTIC_SEARCH_QUERY = "ElasticSearchQuery"
    SIMILARITY_QUERY = "SimilarityQuery"
    FORGE_SEARCH_QUERY = "ForgeSearchQuery"


class PremiseType(Enum):
    SPARQL_PREMISE = "SparqlPremise"
    ELASTIC_SEARCH_PREMISE = "ElasticSearchPremise"
    FORGE_SEARCH_PREMISE = "ForgeSearchPremise"


def format_parameter(provided_value, parameter_type, query_type, forge):
    sparql_valid = [
        PremiseType.SPARQL_PREMISE,
        QueryType.SPARQL_QUERY
    ]

    def format_value(format_str, value, expand_uri=False):
        def format_singular(x):
            if expand_uri:
                x = _expand_uri(forge, x)
            return format_str.format(x) if format_str else x

        return [format_singular(el) for el in value] if isinstance(value, list) else format_singular(value)

    def format_list_value(provided_value, format_string=None, join_string=None, wrap_string=None, expand=False):

        final_value = provided_value

        if format_string:
            final_value = format_value(format_string, final_value, expand_uri=expand)
        if join_string:
            final_value = join_string.join(final_value)
        if wrap_string:
            final_value = format_value(wrap_string, value=final_value, expand_uri=False)

        return final_value

    list_types = [ParameterType.LIST, ParameterType.URI_LIST, ParameterType.SPARQL_LIST,
                  ParameterType.SPARQL_VALUE_LIST, ParameterType.SPARQL_VALUE_URI_LIST]

    if parameter_type in list_types:

        if parameter_type == ParameterType.LIST:
            format_string, join_string, expand, wrap_string = "\"{}\"", ", ", False, None

        elif parameter_type == ParameterType.URI_LIST:
            if query_type in sparql_valid:
                format_string, join_string, expand, wrap_string = "<{}>", ", ", True, None

            else:
                # TODO: figure out if we need to expand uris
                #  when doing ElasticSearch queries
                #  (hard to say in general because it depends on the indexing)
                format_string, join_string, expand, wrap_string = "\"{}\"", ", ", False, None

        elif parameter_type == ParameterType.SPARQL_LIST:
            format_string, join_string, expand, wrap_string = "<{}>", ", ", False,  "({})"

        elif parameter_type == ParameterType.SPARQL_VALUE_LIST:
            if query_type not in sparql_valid:
                raise InvalidParameterTypeException(parameter_type, query_type)

            format_string, join_string, expand, wrap_string = "(\"{}\")", "\n", False, None

        elif parameter_type == ParameterType.SPARQL_VALUE_URI_LIST:
            if query_type not in sparql_valid:
                raise InvalidParameterTypeException(parameter_type, query_type)

            format_string, join_string, expand, wrap_string = "(<{}>)", "\n", True, None

        else:
            raise UnsupportedTypeException(parameter_type, "parameter type")

        return format_list_value(provided_value, expand=expand, format_string=format_string,
                                 join_string=join_string, wrap_string=wrap_string)
    else:

        if parameter_type == ParameterType.STR:
            value = _enforce_unique(provided_value)
            format_string, expand = "\"{}\"", False

        elif parameter_type == ParameterType.PATH:
            value = _enforce_unique(provided_value)
            format_string, expand = None, False

        elif parameter_type == ParameterType.URI:
            value = _enforce_unique(provided_value)

            # TODO: figure out if we need to expand uris
            #  when doing ElasticSearch queries
            #  (hard to say in general because it depends on the indexing)
            if query_type in [
                PremiseType.ELASTIC_SEARCH_PREMISE,
                QueryType.ELASTIC_SEARCH_QUERY,
                QueryType.SIMILARITY_QUERY
            ]:
                format_string, expand = "\"{}\"", True
            else:
                format_string, expand = None, True
        else:
            raise UnsupportedTypeException(parameter_type, "parameter type")

        return format_value(format_string, value, expand)
