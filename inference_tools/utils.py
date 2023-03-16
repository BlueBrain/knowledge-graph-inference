"""Collection of utils for performing various inference queries."""

import warnings
import getpass
from typing import Dict, Callable, Optional, Type, Union, List

from kgforge.core import KnowledgeGraphForge

from inference_tools.query.elastic_search import ElasticSearch
from inference_tools.query.sparql import Sparql
from inference_tools.query.forge import Forge

from inference_tools.similarity.utils import execute_similarity_query

from inference_tools.exceptions import (InferenceToolsWarning,
                                        InferenceToolsException,
                                        InvalidValueException,
                                        IncompleteObjectException,
                                        InvalidParameterTypeException,
                                        UnsupportedTypeException,
                                        MissingPremiseParameterValue,
                                        InvalidParameterSpecificationException,
                                        ObjectType
                                        )

from inference_tools.multi_predicate_object_pair import (
    multi_predicate_object_pairs_parameter_rewriting,
    multi_predicate_object_pairs_query_rewriting,
    has_multi_predicate_object_pairs
)

from inference_tools.helper_functions import (
    _enforce_unique,
    _enforce_list,
    _safe_get_type_attribute,
    _follow_path,
    _safe_get_id_attribute
)

from inference_tools.type import (ParameterType, QueryType, PremiseType, ObjectTypeSuper)

from inference_tools.parameter import Parameter, ParameterFormatter
from inference_tools.premise_execution import PremiseExecution


def _allocate_forge_session(org: str, project: str, config_file_path: str, endpoint: str = None,
                            search_endpoints: Optional[Dict] = None, token_file_path: str = None):
    """

    @param org:
    @type org: str
    @param project:
    @type project: str
    @param config_file_path:
    @type config_file_path: str
    @param endpoint:
    @type endpoint: str
    @param search_endpoints:
    @type search_endpoints: Optional[Dict]
    @param token_file_path:
    @type token_file_path: str
    @return:
    @rtype: KnowledgeGraphForge
    """
    if token_file_path is not None:
        with open(token_file_path, encoding="utf-8") as f:
            TOKEN = f.read()
    else:
        TOKEN = getpass.getpass()

    ENDPOINT = endpoint if endpoint else "https://bbp.epfl.ch/nexus/v1"

    bucket = f"{org}/{project}"

    DEBUG = False

    if search_endpoints:
        return KnowledgeGraphForge(
            config_file_path,
            endpoint=ENDPOINT,
            token=TOKEN,
            bucket=bucket,
            searchendpoints=search_endpoints,
            debug=DEBUG
        )

    return KnowledgeGraphForge(
        config_file_path,
        endpoint=ENDPOINT,
        token=TOKEN,
        bucket=bucket,
        debug=DEBUG
    )


def _build_parameter_map(forge, parameter_spec, parameter_values, query_type: QueryType,
                         multi=None) -> Dict:
    """

    @param forge:
    @type forge: KnowledgeGraphForge
    @param parameter_spec:
    @type parameter_spec:
    @param parameter_values:
    @type parameter_values:
    @param query_type:
    @type query_type: QueryType
    @param multi:
    @type multi:
    @return:
    @rtype: Dict
    """
    forge = _enforce_unique(forge)

    parameter_spec = _enforce_list(parameter_spec)

    if multi:  # irrelevant for premises, just sparql queries
        (idx, name, nb_multi) = multi
        parameter_spec, parameter_values = \
            multi_predicate_object_pairs_parameter_rewriting(idx,
                                                             parameter_spec, parameter_values)
    try:
        parameter_spec_class: [Parameter] = [Parameter(spec) for spec in parameter_spec]
    except (IncompleteObjectException, InvalidValueException) as e:
        raise InvalidParameterSpecificationException(e.message) from e

    if isinstance(query_type, PremiseType):
        try:
            [p.get_value(parameter_values) for p in parameter_spec_class]
        except IncompleteObjectException as e:
            raise MissingPremiseParameterValue(e.name) from e
            # to raise if a premise can't be ran due to one or many of its parameters missing

    return dict((
                    p.name,
                    ParameterFormatter.format_parameter(
                        parameter_type=p.type,
                        provided_value=p.get_value(parameter_values),
                        query_type=query_type,
                        forge=forge
                    )
                ) for p in parameter_spec_class if p.get_value(parameter_values) is not None)
    # if filters out optional ones


def check_premises(forge_factory: Callable[[str, str], KnowledgeGraphForge], rule: Dict,
                   parameters: Optional[Dict], debug: bool = False):
    """

    @param forge_factory:   A function that takes as an input the name of the organization and
        the project, and returns a forge session.
    @type forge_factory:
    @param rule: JSON-representation of a rule
    @type rule: Dict
    @param parameters: Input parameters the premises will check
    @type parameters: Optional[Dict]
    @param debug: Whether running the premise queries is in debug mode
    @type debug: bool
    @return:
    @rtype: bool
    """

    premises = rule.get("premise", None)

    if premises is None:
        return True
    premises = _enforce_list(premises)

    flags = []

    for premise in premises:

        config = _get_query_configuration(premise, ObjectType.PREMISE)

        forge = forge_factory(config['org'], config['project'])

        premise_type = _get_type(premise, ObjectType.PREMISE, PremiseType)

        if "hasParameter" in premise:
            try:
                current_parameters = _build_parameter_map(
                    forge, premise["hasParameter"], parameters, premise_type)
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

        if premise_type.value in sources.keys():

            source = sources[premise_type.value]

            flag = source.check_premise(
                forge=forge,
                premise=premise,
                parameters=current_parameters,
                config=config,
                debug=debug
            )

            source.restore_default_views(forge)

            flags.append(flag)
            if flag == PremiseExecution.FAIL:
                break
        else:
            raise UnsupportedTypeException(premise_type.value, "premise type")

    if all(flag == PremiseExecution.SUCCESS for flag in flags):
        # All premises are successful
        return True
    if any(flag == PremiseExecution.FAIL for flag in flags):
        # One premise has failed
        return False
    if all(flag == PremiseExecution.MISSING_PARAMETER for flag in flags) and len(parameters) == 0:
        # Nothing is provided, all premises are missing parameters
        return True
    if all(flag == PremiseExecution.MISSING_PARAMETER for flag in flags) and len(parameters) > 0:
        # Things are provided, all premises are missing parameters
        return False
    if all(flag in [PremiseExecution.MISSING_PARAMETER, PremiseExecution.SUCCESS] for flag in
           flags):
        # Some premises are successful, some are missing parameters
        return True

    print(flags)

    return False


def _get_query_configuration(obj: Dict, obj_type: ObjectType) -> Union[Dict, List[Dict]]:
    """

    @param obj:
    @type obj:
    @param obj_type:
    @type obj_type:
    @return:
    @rtype: Union[Dict, List[Dict]]
    """
    config = obj.get("queryConfiguration", None)

    if config is None:
        raise IncompleteObjectException(object_type=obj_type, attribute="queryConfiguration")
    return config


def execute_query_object(forge_factory: Callable[[str, str], KnowledgeGraphForge], query,
                         parameters: Optional[Dict], last_query=False, debug=False) -> List[Dict]:
    """
    Execute an individual query given parameters.

    @param forge_factory:  A function that takes as an input the name of the organization and
    the project, and returns a forge session.
    @type forge_factory: Callable[[str, str], KnowledgeGraphForge]
    @param query: JSON-representation of a query
    @type query: Dict
    @param parameters:
    @type parameters: Optional[Dict]
    @param last_query:
    @type last_query:  bool
    @param debug:
    @type debug: bool
    @return:  List of the result resources
    @rtype: List[Dict]
    """

    config = _get_query_configuration(query, ObjectType.QUERY)

    if isinstance(config, list):
        forge = [
            forge_factory(el["org"], el["project"])
            for el in config
        ]
    else:
        forge = forge_factory(config["org"], config["project"])

    query_type: QueryType = _get_type(query, ObjectType.QUERY, QueryType)

    if "hasParameter" in query:
        multi = has_multi_predicate_object_pairs(query["hasParameter"], parameters)
        if multi:
            if query_type == QueryType.SPARQL_QUERY:
                (idx, name, nb_multi) = multi
                query["hasBody"] = multi_predicate_object_pairs_query_rewriting(name, nb_multi,
                                                                                query["hasBody"])
            else:
                raise InvalidParameterTypeException(ParameterType.MULTI_PREDICATE_OBJECT_PAIR,
                                                    query_type)

        try:
            current_parameters = _build_parameter_map(
                forge, query["hasParameter"], parameters, query_type, multi=multi)
        except InferenceToolsException as e:
            raise InferenceToolsException(
                "Query cannot be executed, one or more parameters " +
                f"are missing. See the following exception: {e}") from e
    else:
        current_parameters = {}

    sources = {
        QueryType.SPARQL_QUERY.value: Sparql,
        QueryType.FORGE_SEARCH_QUERY.value: Forge,
        QueryType.ELASTIC_SEARCH_QUERY.value: ElasticSearch
    }

    if query_type.value in sources.keys():

        source = sources[query_type.value]

        resources = source.execute_query(
            forge=forge,
            query=query,
            parameters=current_parameters,
            config=config,
            debug=debug
        )

        if query_type == QueryType.ELASTIC_SEARCH_QUERY and last_query:
            resources = [
                {"id": _safe_get_id_attribute(el)}
                for el in resources
            ]

        source.restore_default_views(forge)

    elif query_type == QueryType.SIMILARITY_QUERY:
        resources = execute_similarity_query(
            forge=forge,
            query=query,
            parameters=current_parameters,
            forge_factory=forge_factory,
        )
    else:
        raise UnsupportedTypeException(query_type.value, "query type")

    return resources


def _get_type(obj: Dict, obj_type: ObjectType,
              type_type: Type[ObjectTypeSuper]) -> Union[ParameterType, PremiseType, QueryType]:
    """
    Gets a type from a dictionary, and converts this type to the appropriate enum
    @param obj: the dictionary holding a type field
    @type obj: Dict
    @param obj_type: the type of the dictionary
    @type obj_type: ObjectType
    @param type_type: the enum class for the type => the type of the type
    @type type_type Type[ObjectTypeSuper]
    @return: an instance of type_type
    @rtype: ObjectTypeSuper
    """
    try:
        type_value = _safe_get_type_attribute(obj)
    except TypeError as e:
        raise IncompleteObjectException(object_type=obj_type, attribute="type") from e

    try:
        return type_type(type_value)
    except ValueError as e:
        raise InvalidValueException(attribute=f"{obj_type.value} type", value=type_value) from e


def execute_query_pipe(forge_factory: Callable[[str, str], KnowledgeGraphForge],
                       head: Dict, parameters: Optional[Dict], rest: Optional[Dict],
                       debug: bool = False):
    """
    Execute a query pipe given the input parameters.

    This recursive function executes pipes of queries and performs
    parameter building between each individual query.

    @param forge_factory: A function that takes as an input the name of the organization and
        the project, and returns a forge session.
    @type forge_factory: Callable[[str, str], KnowledgeGraphForge]
    @param head: JSON-representation of a head query
    @type head: Dict
    @param parameters: Input parameter dictionary to use in the queries.
    @type parameters: Optional[Dict]
    @param rest:JSON-representation of the remaining query or query pipe
    @type rest: Optional[Dict]
    @param debug:   Whether to run queries in debug mode
    @type debug: bool
    @return:
    @rtype:
    """

    if rest is None:
        try:
            head_type = _safe_get_type_attribute(head)
        except TypeError as e:
            raise IncompleteObjectException(attribute="head type",
                                            object_type=ObjectType.QUERY_PIPE) from e

        if head_type == "QueryPipe":
            return execute_query_pipe(
                forge_factory=forge_factory, head=head["head"],
                parameters=parameters, rest=head["rest"],
                debug=debug)

        return execute_query_object(forge_factory=forge_factory, query=head, parameters=parameters,
                                    debug=debug, last_query=True)

    result = execute_query_object(forge_factory=forge_factory, query=head, parameters=parameters,
                                  debug=debug)

    if not result:
        return []

    # Compute new parameters combining old parameters and the result
    new_parameters = {**parameters}

    result_parameter_mapping = _enforce_list(head["resultParameterMapping"])

    for mapping in result_parameter_mapping:
        if isinstance(result, list):
            new_parameters[mapping["parameterName"]] = [
                _follow_path(el, mapping["path"]) for el in result
            ]
        else:
            new_parameters[mapping["parameterName"]] = \
                result[mapping["path"]]

    try:
        rest_type = _safe_get_type_attribute(rest)
    except TypeError as e:
        raise IncompleteObjectException(attribute="rest type",
                                        object_type=ObjectType.QUERY_PIPE) from e

    if rest_type == "QueryPipe":
        return execute_query_pipe(
            forge_factory=forge_factory, head=rest["head"], parameters=new_parameters,
            rest=rest["rest"], debug=debug
        )

    return execute_query_object(
        forge_factory, rest, new_parameters, last_query=True, debug=debug)


def apply_rule(forge_factory: Callable[[str, str], KnowledgeGraphForge], rule, parameters,
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
    @param parameters: Parameter dictionary to use in premises and search queries.
    @type parameters: Dict
    @param premise_check:
    @type premise_check: bool
    @param debug:
    @type debug: bool
    @return: The list of inference resources' ids, if any
    @rtype: List[Dict]
    """

    satisfies = check_premises(forge_factory=forge_factory, rule=rule,
                               parameters=parameters, debug=debug) \
        if \
        premise_check \
        else True

    if satisfies:
        return execute_query_pipe(
            forge_factory=forge_factory, head=rule["searchQuery"], parameters=parameters, rest=None,
            debug=debug)

    warnings.warn(
        "Rule premise is not satisfied on the input parameters",
        InferenceToolsWarning)

    return []


def get_premise_parameters(rule: Dict) -> Dict:
    """
    Get all input parameters in the premises

    @param rule:
    @type rule: Dict
    @return: the input parameters of the premises
    @rtype: Dict
    """

    if "premise" not in rule:
        return {}

    premises = _enforce_list(rule["premise"])

    premise_params = {}

    for premise in premises:
        if "hasParameter" in premise:
            params = _enforce_list(premise["hasParameter"])
            premise_i_params = dict((p["name"], p) for p in params)
            premise_params.update(premise_i_params)

    return premise_params


def get_query_pipe_params(query: Dict) -> Dict:
    """
    Get all input parameters in a query pipe

    @param query:
    @type query: Dict
    @return: The input parameters of the query pipe
    @rtype: Dict
    """

    def _get_input_params(input_params, prev_output_params=None):

        if prev_output_params is None:
            prev_output_params = []

        new_params = _enforce_list(input_params)

        return dict((p["name"], p) for p in new_params if p["name"] not in prev_output_params)

    def _get_output_params(sub_query):
        param_mapping = sub_query.get("resultParameterMapping", [])
        if len(param_mapping) == 0:
            return []
        return [p["parameterName"] for p in _enforce_list(param_mapping)]

    def _get_head_rest(sub_query):
        try:
            query_type = _safe_get_type_attribute(sub_query)
        except TypeError as e:
            raise IncompleteObjectException(object_type=ObjectType.QUERY, attribute="type") from e

        if query_type == "QueryPipe":
            return sub_query["head"], sub_query["rest"]

        return sub_query, None

    input_parameters = {}
    output_parameters = []
    rest = query

    while rest is not None:
        head, rest = _get_head_rest(rest)
        input_parameters.update(_get_input_params(head.get("hasParameter", []), output_parameters))
        output_parameters += _get_output_params(head)

    return input_parameters


def get_rule_parameters(rule) -> Dict:
    """
    Get parameters of the input rule, union of both premise parameters and query parameters

    @param rule: JSON-representation of a rule
    @type rule: Dict
    @return: Dictionary with all the input parameters. Keys are parameter names,
        values are full parameter payloads.
    @rtype: Dict
    """

    # Get premise parameters
    premise_params = get_premise_parameters(rule)

    # Get query parameters
    search_query = rule.get("searchQuery", None)

    if search_query is None:
        raise IncompleteObjectException(name=rule["name"],
                                        attribute="searchQuery",
                                        object_type=ObjectType.RULE)

    search_params = get_query_pipe_params(search_query)

    return {**premise_params, **search_params}
