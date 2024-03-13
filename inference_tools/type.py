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

"""
Describes a set of types for some objects inside the rule
"""
from enum import Enum
from typing import Union


class RuleType(Enum):
    """
    The different values a Rule type can take
    """
    DATA_GENERALIZATION_RULE = "DataGeneralizationRule"
    EMBEDDING_BASED_GENERALIZATION_RULE = "EmbeddingBasedGeneralizationRule"
    RESOURCE_GENERALIZATION_RULE = "ResourceGeneralizationRule"
    HIERARCHY_BASED_GENERALIZATION_RULE = "HierarchyBasedGeneralizationRule"


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


ObjectType = Union[QueryType, PremiseType, ParameterType]


class ObjectTypeStr(Enum):
    """
    Helper enum used when manipulating dictionaries. When a dictionary is expected to be
    of a specific type, if it does not match expectations,
    the enum value will be used in the message of the exception being thrown.
    """
    QUERY_PIPE = "query pipe"
    PARAMETER = "parameter"
    RULE = "rule"
    QUERY = "query"
    PREMISE = "premise"
    QUERY_CONFIGURATION = "query configuration"
