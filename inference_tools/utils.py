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

"""Collection of utils for performing various inference queries."""

from typing import Dict, Optional, Union, List, Tuple

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.parameter_specification import ParameterSpecification
from inference_tools.datatypes.query import Query, SimilaritySearchQuery
from inference_tools.datatypes.query_configuration import SimilaritySearchQueryConfiguration
from inference_tools.datatypes.query_pipe import QueryPipe
from inference_tools.datatypes.rule import Rule

from inference_tools.exceptions.exceptions import (
    IncompleteObjectException,
    MissingPremiseParameterValue,
    ObjectTypeStr
)

from inference_tools.helper_functions import _enforce_unique
from inference_tools.multi_predicate_object_pair import multi_check

from inference_tools.type import (QueryType, PremiseType)

from inference_tools.parameter_formatter import ParameterFormatter


def _build_parameter_map(forge: KnowledgeGraphForge, parameter_spec: List[ParameterSpecification],
                         parameter_values: Dict,
                         query_type: Union[QueryType, PremiseType]) -> Dict:
    """

    @param forge:
    @type forge: KnowledgeGraphForge
    @param parameter_spec:
    @type parameter_spec: List[ParameterSpecification]
    @param parameter_values:
    @type parameter_values: Dict
    @param query_type:
    @type query_type: Union[QueryType, PremiseType]
    @return:
    @rtype: Dict
    """

    forge = _enforce_unique(forge)

    if isinstance(query_type, PremiseType):
        try:
            [p.get_value(parameter_values) for p in parameter_spec]
        except IncompleteObjectException as e:
            raise MissingPremiseParameterValue(e.name) from e
            # to raise if a premise can't be ran due to one or many of its parameters missing

    return dict(
        (
            p.name,
            ParameterFormatter.format_parameter(
                parameter_type=p.type,
                provided_value=p.get_value(parameter_values),
                query_type=query_type,
                forge=forge
            )
        )
        for p in parameter_spec if p.get_value(parameter_values) is not None
    )
    # if filters out optional ones


def get_premise_parameters(rule: Rule) -> Dict[str, Dict]:
    """
    Get all input parameters in the premises

    @param rule:
    @type rule: Rule
    @return: the input parameters of the premises of a rule
    @rtype: Dict[str, Dict]
    """

    if rule.premises is None:
        return {}

    return dict((
        (p.name, p.to_dict())
        for premise in rule.premises
        for p in premise.parameter_specifications
    ))


def get_search_query_parameters(rule: Rule) -> Dict[str, ParameterSpecification]:
    """
    Get all input parameters in the search query of a rule

    @param rule:
    @type rule: Rule
    @return: The input parameters of the search query of the rule
    @rtype: Dict
    """

    def _get_input_params(input_params: List[ParameterSpecification], prev_output_params=None) -> \
            Dict[str, ParameterSpecification]:

        if prev_output_params is None:
            prev_output_params = []

        return dict(
            (p.name, p)
            for p in input_params
            if p.name not in prev_output_params
        )

    def _get_output_params(sub_query: Query) -> List[str]:
        if sub_query.result_parameter_mapping is None:
            return []

        return [p.parameter_name for p in sub_query.result_parameter_mapping]

    def _get_head_rest(sub_query: Union[QueryPipe, Query]) -> \
            Tuple[Query, Optional[Union[Query, QueryPipe]]]:

        return (sub_query.head, sub_query.rest) \
            if isinstance(sub_query, QueryPipe) \
            else (sub_query, None)

    if rule.search_query is None:
        raise IncompleteObjectException(
            name=rule.name, attribute="searchQuery", object_type=ObjectTypeStr.RULE
        )

    input_parameters = {}
    output_parameters: List[str] = []
    rest: Optional[Union[Query, QueryPipe]] = rule.search_query

    while rest is not None:
        head, rest = _get_head_rest(rest)
        input_parameters.update(_get_input_params(head.parameter_specifications, output_parameters))
        output_parameters += _get_output_params(head)

    return input_parameters


def get_rule_parameters(rule: Rule) -> Dict:
    """
    Get parameters of the input rule, union of both premise parameters and search query parameters

    @param rule: a rule
    @type rule: Rule
    @return: Dictionary with all the input parameters. Keys are parameter names,
        values are full parameter payloads.
    @rtype: Dict
    """

    return {
        **get_premise_parameters(rule),
        **get_search_query_parameters(rule)
    }


def format_parameters(
        query: Query, parameter_values: Dict, forge: KnowledgeGraphForge
) -> Dict:
    """
    Formats the parameters provided by the user.
    May also alter the body of the query in the case of Sparql queries and the usage of a
    MultiPredicateObjectPair
    @param query: the query, holding the parameter specification and its query body
    @type query: Query
    @param parameter_values: tha parameters provided by the user
    @type parameter_values: Dict
    @param forge:
    @type forge: KnowledgeGraphForge
    @return: the formatted input parameter values
    @rtype: Dict
    """
    if len(query.parameter_specifications) == 0:
        return {}

    # side effect: can rewrite into query body
    parameter_spec, parameter_values = multi_check(parameter_values=parameter_values, query=query)

    parameter_map = _build_parameter_map(
        forge=forge, parameter_spec=parameter_spec,
        parameter_values=parameter_values,
        query_type=query.type
    )

    return parameter_map


# TODO where is it used
def get_embedding_models(rule: Rule) -> Dict[str, Dict]:
    """
    Gets the embedding models inside the search query of a rule, if this search query is
    of type SimilaritySearchQuery. Formats useful information into a dictionary
    @param rule:
    @type rule: Rule
    @return:
    @rtype: Dict[str, Dict]
    """
    if not isinstance(rule.search_query, SimilaritySearchQuery):
        return {}

    def _transform(qc: SimilaritySearchQueryConfiguration):

        e = qc.embedding_model_data_catalog

        if e is not None:
            return (e.name.replace(" ", "_"), {
                "name": e.name,
                "description": e.description,
                "id": e.id,
                "distance": e.distance.value
            })
        return None

    return dict(
        _transform(qc) for qc in rule.search_query.query_configurations
        if _transform(qc) is not None
    )
