import warnings
from typing import Dict, Callable, Optional, Union, List, Tuple
from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.query import Query
from inference_tools.datatypes.query_configuration import QueryConfiguration
from inference_tools.datatypes.query_pipe import QueryPipe
from inference_tools.datatypes.rule import Rule
from inference_tools.source.elastic_search import ElasticSearch
from inference_tools.source.sparql import Sparql
from inference_tools.source.forge import Forge
from inference_tools.source.source import DEFAULT_LIMIT

from inference_tools.similarity.utils import execute_similarity_query

from inference_tools.utils import _build_parameter_map

from inference_tools.exceptions import (
    InferenceToolsWarning,
    InferenceToolsException,
    UnsupportedTypeException,
    MissingPremiseParameterValue,
    InvalidParameterSpecificationException
)

from inference_tools.multi_predicate_object_pair import multi_check

from inference_tools.helper_functions import _follow_path, get_id_attribute, _enforce_list

from inference_tools.type import (QueryType, PremiseType)

from inference_tools.premise_execution import PremiseExecution


def format_parameters(query: Query, parameter_values: Optional[Dict], forge: KnowledgeGraphForge) \
        -> Tuple[Optional[int], Dict]:
    if len(query.parameter_specifications) == 0:
        return None, {}

    # side effect: can rewrite into query body
    parameter_spec, parameter_values = multi_check(parameter_values=parameter_values, query=query)

    limit = parameter_values.get("LimitQueryParameter", DEFAULT_LIMIT)

    try:
        parameter_map = _build_parameter_map(
            forge=forge, parameter_spec=parameter_spec,
            parameter_values=parameter_values,
            query_type=query.type
        )
    except InferenceToolsException as e:
        raise InferenceToolsException(
            "Query cannot be executed, one or more parameters " +
            f"are missing. See the following exception: {e}") from e

    return limit, parameter_map


def execute_query_object(forge_factory: Callable[[str, str], KnowledgeGraphForge], query: Query,
                         parameter_values: Optional[Dict],
                         last_query=False, debug=False) -> List[Dict]:
    """
    Execute an individual query given parameters.

    @param forge_factory:  A function that takes as an input the name of the organization and
    the project, and returns a forge session.
    @type forge_factory: Callable[[str, str], KnowledgeGraphForge]
    @param query: JSON-representation of a query
    @type query: Query
    @param parameter_values:
    @type parameter_values: Optional[Dict]
    @param last_query:
    @type last_query:  bool
    @param debug:
    @type debug: bool
    @return:  List of the result resources
    @rtype: List[Dict]
    """

    forge = [forge_factory(qc.org, qc.project) for qc in query.query_configurations]
    # Multiple forge configs only for similarity query???

    limit, formatted_parameters = format_parameters(query=query, parameter_values=parameter_values,
                                                    forge=forge[0])

    sources = {
        QueryType.SPARQL_QUERY.value: Sparql,
        QueryType.FORGE_SEARCH_QUERY.value: Forge,
        QueryType.ELASTIC_SEARCH_QUERY.value: ElasticSearch
    }

    if query.type.value in sources.keys():

        source = sources[query.type.value]

        # TODO are multiple query configurations only applicable to similarity queries?
        resources = source.execute_query(
            forge=forge[0],
            query=query,
            parameter_values=formatted_parameters,
            config=query.query_configurations[0],
            debug=debug,
            limit=limit if last_query else None
        )

        if query.type == QueryType.ELASTIC_SEARCH_QUERY and last_query:
            resources = [
                {"id": get_id_attribute(el)}
                for el in resources
            ]

        for f in forge:
            source.restore_default_views(f)

    elif query.type == QueryType.SIMILARITY_QUERY:
        resources = execute_similarity_query(
            forge=forge,
            query=query,
            parameter_values=formatted_parameters,
            forge_factory=forge_factory,
        )
    else:
        raise UnsupportedTypeException(query.type.value, "query type")

    return resources


def apply_rule(forge_factory: Callable[[str, str], KnowledgeGraphForge], rule: Dict,
               parameter_values: Dict,
               premise_check=True, debug=False) -> List[Dict]:
    """
    Apply a rule given the input parameters.
    This function, first, checks if the premises of the rule are satisfied.
    Then runs the search query or query pipe.

    @param forge_factory: A function that takes as an input the name of the organization and
    the project, and returns a forge session.
    @type forge_factory:  Callable[[str, str], KnowledgeGraphForge]
    @param rule: JSON-representation of a rule
    @type rule: Dict
    @param parameter_values: Parameter dictionary to use in premises and search queries.
    @type parameter_values: Dict
    @param premise_check:
    @type premise_check: bool
    @param debug:
    @type debug: bool
    @return: The list of inference resources' ids, if any
    @rtype: List[Dict]
    """

    rule = Rule(rule)

    satisfies = check_premises(forge_factory=forge_factory, rule=rule,
                               parameter_values=parameter_values, debug=debug) \
        if premise_check else True

    if not satisfies:
        warnings.warn(
            "Rule premise is not satisfied on the input parameters",
            InferenceToolsWarning)

        return []

    return execute_query_pipe(
        forge_factory=forge_factory, head=rule.search_query,
        parameter_values=parameter_values, rest=None,
        debug=debug
    )


def execute_query_pipe(forge_factory: Callable[[str, str], KnowledgeGraphForge],
                       head: Union[Query, QueryPipe], parameter_values: Optional[Dict],
                       rest: Optional[Union[Query, QueryPipe]], debug: bool = False):
    """
    Execute a query pipe given the input parameters.

    This recursive function executes pipes of queries and performs
    parameter building between each individual query.

    @param forge_factory: A function that takes as an input the name of the organization and
        the project, and returns a forge session.
    @type forge_factory: Callable[[str, str], KnowledgeGraphForge]
    @param head: JSON-representation of a head query
    @type head: Dict
    @param parameter_values: Input parameter dictionary to use in the queries.
    @type parameter_values: Optional[Dict]
    @param rest:JSON-representation of the remaining query or query pipe
    @type rest: Optional[Dict]
    @param debug:   Whether to run queries in debug mode
    @type debug: bool
    @return:
    @rtype:
    """

    if rest is None:

        if isinstance(head, QueryPipe):
            return execute_query_pipe(
                forge_factory=forge_factory, head=head.head,
                parameter_values=parameter_values, rest=head.rest,
                debug=debug)

        return execute_query_object(forge_factory=forge_factory, query=head,
                                    parameter_values=parameter_values,
                                    debug=debug, last_query=True)

    result = execute_query_object(forge_factory=forge_factory, query=head,
                                  parameter_values=parameter_values,
                                  debug=debug)

    if not result:
        return []

    # Compute new parameters combining old parameters and the result
    new_parameters = {**parameter_values}

    result_parameter_mapping = head.result_parameter_mapping

    for mapping in result_parameter_mapping:
        if isinstance(result, list):
            new_parameters[mapping.parameter_name] = [
                _follow_path(el, mapping.path) for el in result
            ]
        else:
            new_parameters[mapping.parameter_name] = result[mapping.path]

    if isinstance(rest, QueryPipe):
        return execute_query_pipe(
            forge_factory=forge_factory, head=rest.head, parameter_values=new_parameters,
            rest=rest.rest, debug=debug
        )

    return execute_query_object(
        forge_factory=forge_factory, query=rest, parameter_values=new_parameters,
        last_query=True, debug=debug
    )


def check_premises(forge_factory: Callable[[str, str], KnowledgeGraphForge], rule: Rule,
                   parameter_values: Optional[Dict], debug: bool = False):
    """

    @param forge_factory:   A function that takes as an input the name of the organization and
        the project, and returns a forge session.
    @type forge_factory:
    @param rule: JSON-representation of a rule
    @type rule: Dict
    @param parameter_values: Input parameters the premises will check
    @type parameter_values: Optional[Dict]
    @param debug: Whether running the premise queries is in debug mode
    @type debug: bool
    @return:
    @rtype: bool
    """

    if rule.premises is None:
        return True

    flags = []

    for premise in rule.premises:

        config: QueryConfiguration = premise.query_configurations[0]
        forge = forge_factory(config.org, config.project)

        if len(premise.parameter_specifications) > 0:
            try:
                current_parameters = _build_parameter_map(
                    forge, premise.parameter_specifications, parameter_values, premise.type)
            except MissingPremiseParameterValue:
                flags.append(PremiseExecution.MISSING_PARAMETER)
                continue
            except InvalidParameterSpecificationException as e2:
                warnings.warn(e2.message, InferenceToolsWarning)
                flags.append(PremiseExecution.ERROR)
                # TODO invalid premise, independently from input params
                break
        else:
            current_parameters = {}

        sources = {
            PremiseType.SPARQL_PREMISE.value: Sparql,
            PremiseType.FORGE_SEARCH_PREMISE.value: Forge,
            PremiseType.ELASTIC_SEARCH_PREMISE.value: ElasticSearch
        }

        if premise.type.value in sources.keys():

            source = sources[premise.type.value]

            flag = source.check_premise(
                forge=forge,
                premise=premise,
                parameter_values=current_parameters,
                config=config,
                debug=debug
            )

            source.restore_default_views(forge)

            flags.append(flag)
            if flag == PremiseExecution.FAIL:
                break
        else:
            raise UnsupportedTypeException(premise.type.value, "premise type")

    if all(flag == PremiseExecution.SUCCESS for flag in flags):
        # All premises are successful
        return True
    if any(flag == PremiseExecution.FAIL for flag in flags):
        # One premise has failed
        return False
    if all(flag == PremiseExecution.MISSING_PARAMETER for flag in flags):
        if len(parameter_values) == 0:
            # Nothing is provided, all premises are missing parameters
            return True

        if any(len(value_set) > 0 for value_set in
               [_enforce_list(data) for data in parameter_values.values() if data is not None]
               ):
            # Parameter values are provided, all premises are missing parameters
            return False

        # Things are provided, but the values are empty, all premises are missing parameters
        return True

    if all(flag in [PremiseExecution.MISSING_PARAMETER, PremiseExecution.SUCCESS] for flag in
           flags):
        # Some premises are successful, some are missing parameters
        return True

    return False
