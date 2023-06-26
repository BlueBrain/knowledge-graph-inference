"""
Describes a set of types for some objects inside the rule
"""
from enum import Enum
from typing import NewType, Union


class ParameterType(Enum):
    """
    All types of input parameters that can define a rule
    """
    LIST = "list"
    URI_LIST = "uri_list"
    SPARQL_VALUE_LIST = "sparql_value_list"
    SPARQL_VALUE_URI_LIST = "sparql_value_uri_list"
    SPARQL_LIST = "sparql_list"
    URI = "uri"
    STR = "str"
    PATH = "path"
    BOOL = "bool"
    MULTI_PREDICATE_OBJECT_PAIR = "MultiPredicateObjectPair"
    QUERY_BLOCK = "query_block"


parameter_list_types = [ParameterType.LIST, ParameterType.URI_LIST, ParameterType.SPARQL_LIST,
                        ParameterType.SPARQL_VALUE_LIST, ParameterType.SPARQL_VALUE_URI_LIST]


class QueryType(Enum):
    """
    All types of queries ran that can be executed
    """
    SPARQL_QUERY = "SparqlQuery"
    ELASTIC_SEARCH_QUERY = "ElasticSearchQuery"
    SIMILARITY_QUERY = "SimilarityQuery"
    FORGE_SEARCH_QUERY = "ForgeSearchQuery"


class PremiseType(Enum):
    """
    All types of premises that can be checked
    """
    SPARQL_PREMISE = "SparqlPremise"
    ELASTIC_SEARCH_PREMISE = "ElasticSearchPremise"
    FORGE_SEARCH_PREMISE = "ForgeSearchPremise"


ObjectType = NewType('ObjectType', Union[QueryType, PremiseType, ParameterType])


class ObjectTypeStr(Enum):
    QUERY_PIPE = "query pipe"
    PARAMETER = "parameter"
    RULE = "rule"
    QUERY = "query"
    PREMISE = "premise"
    QUERY_CONFIGURATION = "query configuration"
