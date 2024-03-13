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

from typing import Dict, Callable, Optional, Union, List, Type

from kgforge.core import KnowledgeGraphForge, Resource

from inference_tools.datatypes.parameter_mapping import ParameterMapping
from inference_tools.datatypes.query import Query, SimilaritySearchQuery
from inference_tools.datatypes.query_configuration import QueryConfiguration
from inference_tools.datatypes.query_pipe import QueryPipe
from inference_tools.datatypes.rule import Rule
from inference_tools.exceptions.exceptions import (
    UnsupportedTypeException,
    MissingPremiseParameterValue, FailedQueryException, SimilaritySearchException,
    InferenceToolsException
)
from inference_tools.source.elastic_search import ElasticSearch
from inference_tools.source.sparql import Sparql
from inference_tools.source.forge import Forge

from inference_tools.exceptions.malformed_rule import InvalidParameterSpecificationException
from inference_tools.exceptions.premise import IrrelevantPremiseParametersException
from inference_tools.exceptions.premise import UnsupportedPremiseCaseException, \
    FailedPremiseException, MalformedPremiseException
from inference_tools.helper_functions import _follow_path, _enforce_list
from inference_tools.premise_execution import PremiseExecution
from inference_tools.similarity.main import execute_similarity_query
from inference_tools.source.source import DEFAULT_LIMIT, Source
from inference_tools.type import QueryType, PremiseType
from inference_tools.utils import _build_parameter_map, format_parameters


sources: Dict[Union[QueryType, PremiseType], Type[Source]] = {
    QueryType.SPARQL_QUERY: Sparql,  # type: ignore
    QueryType.FORGE_SEARCH_QUERY: Forge,  # type: ignore
    QueryType.ELASTIC_SEARCH_QUERY: ElasticSearch,  # type: ignore
    PremiseType.SPARQL_PREMISE: Sparql,  # type: ignore
    PremiseType.FORGE_SEARCH_PREMISE: Forge,  # type: ignore
    PremiseType.ELASTIC_SEARCH_PREMISE: ElasticSearch  # type: ignore
}


def get_limit(parameter_values: Optional[Dict]):
    """
    Look into optionally user-provided parameter values for max number of results
    to apply on inference.
    @param parameter_values:
    @type parameter_values: Dict
    @return: the user provided limit, or a default value of 20
    @rtype: int
    """
    limit = parameter_values.get("LimitQueryParameter", DEFAULT_LIMIT) \
        if parameter_values else DEFAULT_LIMIT
    if not isinstance(limit, int):
        limit = DEFAULT_LIMIT
    return limit


def execute_query_object(
        forge_factory: Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge],
        query: Query,
        parameter_values: Optional[Dict],
        last_query=False,
        debug=False,
        use_resources: bool = False
) -> List[Dict]:
    """
    Execute an individual query given parameters.

    @param forge_factory:  A function that takes as an input the name of the organization and
    the project, and returns a forge session.
    @type forge_factory: Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge]
    @param query: JSON-representation of a query
    @type query: Query
    @param parameter_values:
    @type parameter_values: Optional[Dict]
    @param last_query:
    @type last_query:  bool
    @param debug:
    @type debug: bool
    @param use_resources:
    @type use_resources: bool
    @return:  List of the result resources
    @rtype: List[Dict]
    """

    limit = get_limit(parameter_values)

    source: Optional[Type[Source]] = sources.get(query.type, None)

    if source:

        query_config_0 = query.query_configurations[0]

        forge = query_config_0.use_factory(forge_factory)

        formatted_parameters = format_parameters(
            query=query, parameter_values=parameter_values or {}, forge=forge
        )

        resources = source.execute_query(
            forge=forge,
            query=query,
            parameter_values=formatted_parameters,
            config=query_config_0,
            debug=debug,
            limit=limit if last_query else None
        )

        if resources is None:
            raise FailedQueryException(description=query.description)

        if isinstance(resources, Resource) or all(isinstance(r, Resource) for r in resources):
            resources = forge.as_json(resources)

        if last_query:

            if not query.result_parameter_mapping or len(query.result_parameter_mapping) == 0:
                return resources

            mapping = process_result_parameter_mapping(
                result_parameter_mapping=query.result_parameter_mapping, result=resources
            )

            return [
                dict((k, v[idx]) for k, v in mapping.items())
                for idx in range(len(resources))
            ]

    elif isinstance(query, SimilaritySearchQuery):
        if parameter_values is None:
            raise SimilaritySearchException(
                "Cannot run similarity search query without input parameters"
            )

        resources = execute_similarity_query(
            query=query,
            parameter_values=parameter_values,
            forge_factory=forge_factory,
            debug=debug,
            use_resources=use_resources,
            limit=limit
        )  # TODO better error handling here
    else:
        raise UnsupportedTypeException(query.type.value, "query type")

    return resources


def apply_rule(
        forge_factory: Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge],
        rule: Dict,
        parameter_values: Dict,
        premise_check: bool = True, debug: bool = False, use_resources: bool = True
) -> List[Dict]:
    """
    Apply a rule given the input parameters.
    This function, first, checks if the premises of the rule are satisfied.
    Then runs the search query or query pipe.

    @param forge_factory: A function that takes as an input the name of the organization and
    the project, and returns a forge session.
    @type forge_factory:  Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge]
    @param rule: JSON-representation of a rule
    @type rule: Dict
    @param parameter_values: Parameter dictionary to use in premises and search queries.
    @type parameter_values: Dict
    @param premise_check:
    @type premise_check: bool
    @param debug:
    @type debug: bool
    @param use_resources:
    @type use_resources: bool
    @return: The list of inference resources' ids, if any
    @rtype: List[Dict]
    """

    rule_object = Rule(rule)

    if premise_check:
        check_premises(
            forge_factory=forge_factory, rule=rule_object,
            parameter_values=parameter_values, debug=debug
        )

    return execute_query_pipe(
        forge_factory=forge_factory, head=rule_object.search_query,
        parameter_values=parameter_values, rest=None,
        debug=debug, use_resources=use_resources
    )


def process_result_parameter_mapping(
        result_parameter_mapping: List[ParameterMapping], result: List[Dict]
):
    """
    In the results of a query ahead in the query pipe, follows paths of interest and
    puts the values there into a field of choice
    @param result_parameter_mapping:
    @type result_parameter_mapping: List[ParameterMapping]
    @param result:
    @type result: List[Dict]
    @return: For each parameter mapping, a list of values
    @rtype: Dict[str, List]
    """
    return dict(
        (mapping.parameter_name, [_follow_path(el, mapping.path) for el in result])
        for mapping in result_parameter_mapping
    )


def combine_parameters(
        result_parameter_mapping: Optional[List[ParameterMapping]], parameter_values: Optional[Dict],
        result: List
) -> Dict:  # TODO enforce result to be a list # used to be a check
    """
    Combine user specified parameter values with parameter values that come
    from the execution of a query that is ahead in the query pipe
    @param result_parameter_mapping: a mapping indicating where to retrieve values in the
    result, and to map them to what kind of parameter, in order for them to become parameter values
    for the next query
    @type result_parameter_mapping:
    @param parameter_values: user specified parameter values
    @type parameter_values: Dict
    @param result:
    @type result: List
    @return:
    @rtype:
    """
    if not result_parameter_mapping:
        if parameter_values:
            return parameter_values
        return {}

    mapping_values = process_result_parameter_mapping(result_parameter_mapping, result)

    if not parameter_values:
        return mapping_values

    return {
        **parameter_values,
        **mapping_values
    }


def execute_query_pipe(
        forge_factory: Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge],
        head: Union[Query, QueryPipe], parameter_values: Optional[Dict],
        rest: Optional[Union[Query, QueryPipe]], debug: bool = False, use_resources: bool = False
):
    """
    Execute a query pipe given the input parameters.

    This recursive function executes pipes of queries and performs
    parameter building between each individual query.

    @param forge_factory: A function that takes as an input the name of the organization and
        the project, and returns a forge session.
    @type forge_factory: Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge]
    @param head: JSON-representation of a head query
    @type head: Dict
    @param parameter_values: Input parameter dictionary to use in the queries.
    @type parameter_values: Optional[Dict]
    @param rest:JSON-representation of the remaining query or query pipe
    @type rest: Optional[Dict]
    @param debug:   Whether to run queries in debug mode
    @type debug: bool
    @param use_resources:
    @type use_resources: bool
    @return:
    @rtype:
    """

    def _check(el, params, last_q):

        if isinstance(el, QueryPipe):
            return execute_query_pipe(
                forge_factory=forge_factory, parameter_values=params, debug=debug,
                head=el.head, rest=el.rest, use_resources=use_resources
            )

        return execute_query_object(
            forge_factory=forge_factory, parameter_values=params, debug=debug,
            query=el, last_query=last_q, use_resources=use_resources
        )  # TODO try catch??

    last_query = rest is None
    result = _check(head, parameter_values, last_q=last_query)

    if last_query:
        return result

    if not isinstance(head, Query):
        raise InferenceToolsException("Unexpected case: combine parameters called on QueryPipe")

    new_parameters = combine_parameters(
        result_parameter_mapping=head.result_parameter_mapping,
        parameter_values=parameter_values, result=result
    )

    return _check(rest, new_parameters, last_q=True)


def check_premises(
        forge_factory: Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge],
        rule: Rule,
        parameter_values: Optional[Dict], debug: bool = False
):
    """

    @param forge_factory: A function that takes as an input the name of the organization and
        the project, and returns a forge session.
    @type forge_factory:
    @param rule: JSON-representation of a rule
    @type rule: Dict
    @param parameter_values: Input parameters the premises will check
    @type parameter_values: Optional[Dict]
    @param debug: Whether running the premise queries is in debug mode
    @type debug: bool
    @return:
    @raise PremiseException
    @rtype: bool
    """

    if rule.premises is None:
        return True

    flags = []

    for premise in rule.premises:

        config: QueryConfiguration = premise.query_configurations[0]
        forge = config.use_factory(forge_factory)

        if len(premise.parameter_specifications) > 0 and parameter_values is not None:
            try:
                current_parameters = _build_parameter_map(
                    forge, premise.parameter_specifications, parameter_values, premise.type
                )
            except MissingPremiseParameterValue:
                flags.append(PremiseExecution.MISSING_PARAMETER)
                continue
            except InvalidParameterSpecificationException as e2:
                raise MalformedPremiseException(e2.message) from e2
                # TODO invalid premise, independently from input params
        else:
            current_parameters = {}

        source: Optional[Type[Source]] = sources.get(premise.type, None)

        if not source:
            raise UnsupportedTypeException(premise.type.value, "premise type")

        flag = source.check_premise(
            forge=forge,
            premise=premise,
            parameter_values=current_parameters,
            config=config,
            debug=debug
        )

        flags.append(flag)

        if flag == PremiseExecution.FAIL:
            raise FailedPremiseException(description=premise.description)

    if all(flag == PremiseExecution.SUCCESS for flag in flags):
        # All premises are successful
        return True

    # if any(flag == PremiseExecution.FAIL for flag in flags):
    #     # One premise has failed
    #     return False
    if all(flag == PremiseExecution.MISSING_PARAMETER for flag in flags):
        if (parameter_values and len(parameter_values) == 0) or not parameter_values:
            # Nothing is provided, all premises are missing parameters
            return True

        if any(len(value_set) > 0 for value_set in
               [_enforce_list(data) for data in parameter_values.values() if data is not None]
               ):
            # Parameter values are provided, all premises are missing parameters
            raise IrrelevantPremiseParametersException()

        # Things are provided, but the values are empty, all premises are missing parameters
        return True

    if all(flag in [PremiseExecution.MISSING_PARAMETER, PremiseExecution.SUCCESS] for flag in
           flags):
        # Some premises are successful, some are missing parameters
        return True

    raise UnsupportedPremiseCaseException(flags)
