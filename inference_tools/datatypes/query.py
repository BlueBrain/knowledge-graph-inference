from typing import List, Optional, Any, Dict, NewType, Union

from inference_tools.helper_functions import _enforce_list, _get_type
from inference_tools.type import QueryType, ObjectTypeStr, PremiseType
from inference_tools.datatypes.parameter_mapping import ParameterMapping
from inference_tools.datatypes.parameter_specification import ParameterSpecification

from inference_tools.datatypes.query_configuration import (
    QueryConfiguration,
    SparqlQueryConfiguration,
    ElasticSearchQueryConfiguration,
    SimilaritySearchQueryConfiguration,
    ForgeQueryConfiguration
)
from inference_tools.exceptions.exceptions import (
    IncompleteObjectException,
    InferenceToolsException,
    InvalidValueException
)


class SparqlQueryBody:
    def __init__(self, body_dict):
        self.query_string = body_dict["query_string"]


ElasticSearchQueryBody = NewType('ElasticSearchQueryBody', Dict)
ForgeQueryBody = NewType("ForgeQueryBody", Dict)
SimilaritySearchQueryBody = NewType("SimilaritySearchQueryBody", Any)


def premise_factory(obj):
    premise_type = _get_type(obj, obj_type=ObjectTypeStr.PREMISE, type_type=PremiseType)
    if premise_type == PremiseType.SPARQL_PREMISE:
        return SparqlQuery(obj)
    if premise_type == PremiseType.FORGE_SEARCH_PREMISE:
        return ForgeQuery(obj)
    if premise_type == PremiseType.ELASTIC_SEARCH_PREMISE:
        return ElasticSearchQuery(obj)
    raise InferenceToolsException(f"Unsupported premise type {premise_type.value}")


def query_factory(obj):
    query_type = _get_type(obj, obj_type=ObjectTypeStr.QUERY, type_type=QueryType)
    if query_type == QueryType.SPARQL_QUERY:
        return SparqlQuery(obj)
    if query_type == QueryType.SIMILARITY_QUERY:
        return SimilaritySearchQuery(obj)
    if query_type == QueryType.ELASTIC_SEARCH_QUERY:
        return ElasticSearchQuery(obj)
    if query_type == QueryType.FORGE_SEARCH_QUERY:
        return ForgeQuery(obj)
    raise InferenceToolsException(f"Unsupported query type {query_type.value}")


class QuerySuper:

    type: QueryType
    parameter_specifications: List[ParameterSpecification]
    result_parameter_mapping: Optional[List[ParameterMapping]]
    query_configurations: List[QueryConfiguration]
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


class ForgeQuery(QuerySuper):
    body: ForgeQueryBody

    targetParameter: str  # For forge premises
    targetPath: str

    def __init__(self, obj):
        super().__init__(obj)
        self.body = ForgeQueryBody(obj.get("pattern", None))
        self.targetParameter = obj.get("targetParameter", None)
        self.targetPath = obj.get("targetPath", None)

        tmp_qc = obj.get("queryConfiguration", None)
        if tmp_qc is None:
            raise IncompleteObjectException(object_type=ObjectTypeStr.QUERY,
                                            attribute="queryConfiguration")

        self.query_configurations = [
            ForgeQueryConfiguration(obj_i, ObjectTypeStr.QUERY)
            for obj_i in _enforce_list(tmp_qc)
        ]


class SparqlQuery(QuerySuper):

    body: SparqlQueryBody
    query_configurations: List[SparqlQueryConfiguration]

    def __init__(self, obj):
        super().__init__(obj)

        self.body = SparqlQueryBody(obj.get("hasBody", None))

        tmp_qc = obj.get("queryConfiguration", None)
        if tmp_qc is None:
            raise IncompleteObjectException(object_type=ObjectTypeStr.QUERY,
                                            attribute="queryConfiguration")

        self.query_configurations = [
            SparqlQueryConfiguration(obj_i, ObjectTypeStr.QUERY)
            for obj_i in _enforce_list(tmp_qc)
        ]


class ElasticSearchQuery(QuerySuper):

    body: ElasticSearchQueryBody
    query_configurations: List[ElasticSearchQueryConfiguration]

    def __init__(self, obj):
        super().__init__(obj)
        self.body = ElasticSearchQueryBody(obj.get("hasBody", None))

        tmp_qc = obj.get("queryConfiguration", None)
        if tmp_qc is None:
            raise IncompleteObjectException(object_type=ObjectTypeStr.QUERY,
                                            attribute="queryConfiguration")

        self.query_configurations = [
            ElasticSearchQueryConfiguration(obj_i, ObjectTypeStr.QUERY)
            for obj_i in _enforce_list(tmp_qc)
        ]


class SimilaritySearchQuery(QuerySuper):

    body: SimilaritySearchQueryBody
    search_target_parameter: str
    result_filter: str
    query_configurations: List[SimilaritySearchQueryConfiguration]

    def __init__(self, obj):
        super().__init__(obj)
        self.body = SimilaritySearchQueryBody(obj.get("hasBody", None))
        self.search_target_parameter = obj.get("searchTargetParameter", None)
        self.result_filter = obj.get("resultFilter", "")

        tmp_qc = obj.get("queryConfiguration", None)
        if tmp_qc is None:
            raise IncompleteObjectException(object_type=ObjectTypeStr.QUERY,
                                            attribute="queryConfiguration")

        self.query_configurations = [
            SimilaritySearchQueryConfiguration(obj_i, ObjectTypeStr.QUERY)
            for obj_i in _enforce_list(tmp_qc)
        ]


Query = NewType("Query", Union[SparqlQuery, ElasticSearchQuery, SimilaritySearchQuery, ForgeQuery])
