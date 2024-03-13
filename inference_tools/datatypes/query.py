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

from abc import ABC
from typing import List, Optional, Dict, NewType, Sequence

from inference_tools.helper_functions import _enforce_list, _get_type
from inference_tools.type import QueryType, ObjectTypeStr, PremiseType
from inference_tools.datatypes.parameter_mapping import ParameterMapping
from inference_tools.datatypes.parameter_specification import ParameterSpecification
from inference_tools.datatypes.query_configuration import (
    SparqlQueryConfiguration,
    ElasticSearchQueryConfiguration,
    SimilaritySearchQueryConfiguration,
    ForgeQueryConfiguration,
    QueryConfiguration
)
from inference_tools.exceptions.exceptions import (
    IncompleteObjectException,
    InferenceToolsException,
    InvalidValueException
)


class SparqlQueryBody:
    def __init__(self, body_dict):
        self.query_string = body_dict["query_string"]

    def __repr__(self):
        return self.query_string


ElasticSearchQueryBody = NewType('ElasticSearchQueryBody', Dict)
ForgeQueryBody = NewType("ForgeQueryBody", Dict)


class Query(ABC):
    type: QueryType
    parameter_specifications: List[ParameterSpecification]
    result_parameter_mapping: Optional[List[ParameterMapping]]
    query_configurations: Sequence[QueryConfiguration]
    description: Optional[str]

    def __init__(self, obj):

        try:
            self.type = _get_type(obj, ObjectTypeStr.QUERY, QueryType)
        except InvalidValueException:
            try:
                self.type = _get_type(obj, ObjectTypeStr.PREMISE, PremiseType)
            except InvalidValueException as e:
                raise InvalidValueException from e

        self.description = obj.get("description", "No description")
        tmp_param = obj.get("hasParameter", [])
        self.parameter_specifications = [
            ParameterSpecification(obj_i)
            for obj_i in _enforce_list(tmp_param)
        ]

        tmp = obj.get("resultParameterMapping", None)
        self.result_parameter_mapping = [ParameterMapping(obj_i) for obj_i in _enforce_list(tmp)] \
            if tmp is not None else None

    def __repr__(self):
        type_str = f"Type: {self.type.value}"
        desc_str = f"Description: {self.description}"
        param_spec_str = f"Parameter specifications: {self.parameter_specifications}"
        result_param_mapping = f"Result Parameter Mapping: {self.result_parameter_mapping}"
        return "\n".join([type_str, desc_str, param_spec_str, result_param_mapping])


class ForgeQuery(Query):
    body: ForgeQueryBody

    target_parameter: str  # For forge premises
    target_path: str

    def __init__(self, obj):
        super().__init__(obj)
        self.body = ForgeQueryBody(obj.get("pattern", None))
        self.target_parameter = obj.get("targetParameter", None)
        self.target_path = obj.get("targetPath", None)

        tmp_qc = obj.get("queryConfiguration", None)
        if tmp_qc is None:
            raise IncompleteObjectException(
                object_type=ObjectTypeStr.QUERY, attribute="queryConfiguration"
            )

        self.query_configurations = [
            ForgeQueryConfiguration(obj_i) for obj_i in _enforce_list(tmp_qc)
        ]

    def __repr__(self):
        query_super_str = super().__repr__()
        target_parameter_str = f"Target Parameter: {self.target_parameter}"
        target_path_str = f"Target Path: {self.target_path}"
        return "\n".join([query_super_str, target_parameter_str, target_path_str])


class SparqlQuery(Query):

    body: SparqlQueryBody
    query_configurations: List[SparqlQueryConfiguration]

    def __init__(self, obj):
        super().__init__(obj)

        self.body = SparqlQueryBody(obj.get("hasBody", None))

        tmp_qc = obj.get("queryConfiguration", None)
        if tmp_qc is None:
            raise IncompleteObjectException(
                object_type=ObjectTypeStr.QUERY, attribute="queryConfiguration"
            )

        self.query_configurations = [
            SparqlQueryConfiguration(obj_i) for obj_i in _enforce_list(tmp_qc)
        ]

    def __repr__(self):
        query_super_str = super().__repr__()
        sparql_query_str = f"Sparql query: {self.body}"
        qc_str = f"Query configuration: {self.query_configurations}"
        return "\n".join([query_super_str, sparql_query_str, qc_str])


class ElasticSearchQuery(Query):

    body: ElasticSearchQueryBody
    query_configurations: List[ElasticSearchQueryConfiguration]

    def __init__(self, obj):
        super().__init__(obj)
        self.body = ElasticSearchQueryBody(obj.get("hasBody", None))

        tmp_qc = obj.get("queryConfiguration", None)
        if tmp_qc is None:
            raise IncompleteObjectException(
                object_type=ObjectTypeStr.QUERY, attribute="queryConfiguration"
            )

        self.query_configurations = [
            ElasticSearchQueryConfiguration(obj_i)
            for obj_i in _enforce_list(tmp_qc)
        ]

    def __repr__(self):
        query_super_str = super().__repr__()
        es_query_str = f"ES query: {self.body}"
        qc_str = f"Query configuration: {self.query_configurations}"
        return "\n".join([query_super_str, es_query_str, qc_str])


class SimilaritySearchQuery(Query):

    search_target_parameter: str
    result_filter: str
    query_configurations: List[SimilaritySearchQueryConfiguration]

    def __init__(self, obj):
        super().__init__(obj)
        self.search_target_parameter = obj.get("searchTargetParameter", None)
        self.result_filter = obj.get("resultFilter", "")

        tmp_qc = obj.get("queryConfiguration", None)
        if tmp_qc is None:
            raise IncompleteObjectException(
                object_type=ObjectTypeStr.QUERY, attribute="queryConfiguration"
            )

        self.query_configurations = [
            SimilaritySearchQueryConfiguration(obj_i) for obj_i in _enforce_list(tmp_qc)
        ]

    def __repr__(self):
        query_super_str = super().__repr__()
        qc_str = f"Query configurations: {self.query_configurations}"
        result_filter_str = f"Result filter: {self.result_filter}"
        search_target_parameter_str = f"Search Target Parameter: {self.search_target_parameter}"
        return "\n".join([query_super_str, qc_str, result_filter_str, search_target_parameter_str])


def premise_factory(obj: Dict) -> Query:
    """
    Builds a Query object out of a dictionary
    @param obj: the dictionary
    @type obj: Dict
    @return: a premise instance
    @rtype: Query
    @raise InferenceToolsException if the type inside the dictionary is not a valid PremiseType
    """
    premise_type = _get_type(obj, obj_type=ObjectTypeStr.PREMISE, type_type=PremiseType)
    if premise_type == PremiseType.SPARQL_PREMISE:
        return SparqlQuery(obj)
    if premise_type == PremiseType.FORGE_SEARCH_PREMISE:
        return ForgeQuery(obj)
    if premise_type == PremiseType.ELASTIC_SEARCH_PREMISE:
        return ElasticSearchQuery(obj)
    raise InferenceToolsException(f"Unsupported premise type {premise_type.value}")


def query_factory(obj: Dict) -> Query:
    """
       Builds a Query object out of a dictionary
       @param obj: the dictionary
       @type obj: Dict
       @return: a query instance
       @rtype: Query
       @raise InferenceToolsException if the type inside the dictionary is not a valid QueryType
       """
    query_type: QueryType = _get_type(obj, obj_type=ObjectTypeStr.QUERY, type_type=QueryType)  # type: ignore

    query_type_to_class = {
        QueryType.SPARQL_QUERY: SparqlQuery,
        QueryType.ELASTIC_SEARCH_QUERY: ElasticSearchQuery,
        QueryType.FORGE_SEARCH_QUERY: ForgeQuery,
        QueryType.SIMILARITY_QUERY: SimilaritySearchQuery
    }

    class_to_instanciate = query_type_to_class.get(query_type, None)

    if class_to_instanciate is None:
        raise InferenceToolsException(f"Unsupported query type {query_type.value}")

    return class_to_instanciate(obj)
