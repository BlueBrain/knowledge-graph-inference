from enum import Enum

# TODO expose these enums to be able to use them when inference rule writing?? 
# (instead of casting into Enum? & Exception if fail, check they are the appropriate enum?? )

class ParameterType(Enum): 
    LIST = "list"
    URI_LIST = "uri_list"
    SPARQL_VALUE_LIST = "sparql_value_list"
    SPARQL_VALUE_URI_LIST = "sparql_value_uri_list"
    URI = "uri"
    STR = "str"
    PATH = "path"
    MUTLI_PREDICATE_OBJECT_PAIR = "MultiPredicateObjectPair"

class QueryType(Enum): 
    SPARQL_QUERY = "SparqlQuery"
    ELASTIC_SEARCH_QUERY = "ElasticSearchQuery"
    SIMILARITY_QUERY = "SimilarityQuery"
    FORGE_SEARCH_QUERY = "ForgeSearchQuery"
            
class PremiseType(Enum):
    SPARQL_PREMISE = "SparqlPremise"
    ELASTIC_SEARCH_PREMISE = "ElasticSearchPremise"
    FORGE_SEARCH_PREMISE = "ForgeSearchPremise"