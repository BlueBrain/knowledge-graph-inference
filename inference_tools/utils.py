"""Collection of utils for performing various inference queries."""

import warnings

from inference_tools.query.ElasticSearch import ElasticSearch
from inference_tools.query.Sparql import Sparql
from inference_tools.query.Forge import Forge

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

from inference_tools.type import (ParameterType, QueryType, PremiseType)

from kgforge.core import KnowledgeGraphForge
from inference_tools.Parameter import Parameter
from inference_tools.PremiseExecution import PremiseExecution

import getpass


def _allocate_forge_session(org, project, config_file_path, endpoint=None, searchendpoints=None, token_file_path=None):
    if token_file_path is not None:
        with open(token_file_path) as f:
            TOKEN = f.read()
    else:
        TOKEN = getpass.getpass()

    ENDPOINT = endpoint if endpoint else "https://bbp.epfl.ch/nexus/v1"

    bucket = f"{org}/{project}"

    DEBUG = False

    if searchendpoints:

        return KnowledgeGraphForge(
            config_file_path,
            endpoint=ENDPOINT,
            token=TOKEN,
            bucket=bucket,
            searchendpoints=searchendpoints,
            debug=DEBUG
        )
    else:
        return KnowledgeGraphForge(
            config_file_path,
            endpoint=ENDPOINT,
            token=TOKEN,
            bucket=bucket,
            debug=DEBUG
        )


def _build_parameter_map(forge, parameter_spec, parameter_values, query_type, multi=None):
    """Build parameter values given query specification."""

    forge = _enforce_unique(forge)

    parameter_spec = _enforce_list(parameter_spec)

    if multi:  # irrelevant for premises, just sparql queries
        (idx, name, nb_multi) = multi
        parameter_spec, parameter_values = multi_predicate_object_pairs_parameter_rewriting(idx, parameter_spec,
                                                                                            parameter_values)
    try:
        parameter_spec_class: [Parameter] = [Parameter(spec) for spec in parameter_spec]
    except (IncompleteObjectException, InvalidValueException):
        raise InvalidParameterSpecificationException()

    if type(query_type) is PremiseType:
        try:
            [p.get_value(parameter_values) for p in parameter_spec_class]
        except IncompleteObjectException as e:
            raise MissingPremiseParameterValue(e.name)
            # to raise if a premise can't be ran due to one or many of its parameters missing

    param_map = [(
        p.name,
        p.format_parameter(p.get_value(parameter_values), query_type, forge)
    ) for p in parameter_spec_class if p.get_value(parameter_values)]  # if filters out optional ones

    return dict(param_map)


def check_premises(forge_factory, rule, parameters, debug=False):
    """Check if the rule premises are satisfied given the parameters.

    Parameters
    ----------
    forge_factory : func
        A function that takes as an input the name of the organization and
        the project, and returns a forge session.
    rule : dict
        JSON-representation of a rule
    parameters : dict, optional
        Parameter dictionary to use in the premise queries.
    debug: bool, optional
        Whether running the premise queries is in debug mode

    Returns
    -------
    satisfies : bool
    """

    if "premise" not in rule:
        return True

    premises = _enforce_list(rule["premise"])

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
            current_parameters = dict()

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

    if all([flag == PremiseExecution.SUCCESS for flag in flags]):
        # All premises are successful
        return True
    if any([flag == PremiseExecution.FAIL for flag in flags]):
        # One premise has failed
        return False
    if all([flag == PremiseExecution.MISSING_PARAMETER for flag in flags]) and len(parameters) == 0:
        # Nothing is provided, all premises are missing parameters
        return True
    if all([flag == PremiseExecution.MISSING_PARAMETER for flag in flags]) and len(parameters) > 0:
        # Things are provided, all premises are missing parameters
        return False
    if all([flag in [PremiseExecution.MISSING_PARAMETER, PremiseExecution.SUCCESS] for flag in flags]):
        # Some premises are successful, some are missing parameters
        return True

    print(flags)

    return False


def _get_query_configuration(obj, obj_type):
    config = obj.get("queryConfiguration", None)

    if config is None:
        raise IncompleteObjectException(object_type=obj_type, attribute="queryConfiguration")
    return config


def execute_query(forge_factory, query, parameters, last_query=False, debug=False):
    """Execute an individual query given parameters.

    Parameters
    ----------
    forge_factory : func
        A function that takes as an input the name of the organization and
        the project, and returns a forge session.
    query : dict
        JSON-representation of a query
    parameters : dict, optional
        Parameter dictionary to use in the query.
    last_query: bool, optional
    debug: bool, optional

    Returns
    -------
    resources : list
        List of the result resources
    """
    config = _get_query_configuration(query, ObjectType.QUERY)

    if isinstance(config, list):
        forge = [
            forge_factory(el["org"], el["project"])
            for el in config
        ]
    else:
        forge = forge_factory(config["org"], config["project"])

    query_type = _get_type(query, ObjectType.QUERY, QueryType)

    if "hasParameter" in query:
        multi = has_multi_predicate_object_pairs(query["hasParameter"], parameters)
        if multi:
            if query_type == QueryType.SPARQL_QUERY:
                (idx, name, nb_multi) = multi
                query["hasBody"] = multi_predicate_object_pairs_query_rewriting(name, nb_multi, query["hasBody"])
            else:
                raise InvalidParameterTypeException(ParameterType.MULTI_PREDICATE_OBJECT_PAIR, query_type)

        try:
            current_parameters = _build_parameter_map(
                forge, query["hasParameter"], parameters, query_type, multi=multi)
        except InferenceToolsException as e:
            raise InferenceToolsException(
                "Query cannot be executed, one or more parameters " +
                f"are missing. See the following exception: {e}")
    else:
        current_parameters = dict()

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
                {"id": _safe_get_id_attribute(el) }
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


def _get_type(obj, obj_type, type_type):
    try:
        type_value = _safe_get_type_attribute(obj)
    except TypeError:
        raise IncompleteObjectException(object_type=obj_type, attribute="type")

    try:
        type_value = type_type(type_value)
    except ValueError:
        raise InvalidValueException(attribute=f"{obj_type.value} type", value=type_value)

    return type_value


def execute_query_pipe(forge_factory, head, parameters, rest=None, debug=False):
    """Execute a query pipe given the input parameters.

    This recursive function executes pipes of queries and performs
    parameter building between each individual query.

    Parameters
    ----------
    forge_factory : func
        A function that takes as an input the name of the organization and
        the project, and returns a forge session.
    head : dict
        JSON-representation of a head query
    parameters : dict, optional
        Input parameter dictionary to use in the queries.
    rest : dict, optional
        JSON-representation of the remaining query or query pipe
    debug: bool, optional
        Whether to run queries in debug mode
    """
    if rest is None:
        try:
            head_type = _safe_get_type_attribute(head)
        except TypeError:
            raise IncompleteObjectException(attribute="head type", object_type=ObjectType.QUERY_PIPE)

        if head_type == "QueryPipe":
            return execute_query_pipe(
                forge_factory, head["head"], parameters, head["rest"], debug=debug)
        else:
            return execute_query(
                forge_factory, head, parameters, last_query=True, debug=debug)
    else:
        result = execute_query(forge_factory, head, parameters, debug=debug)

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
        except TypeError:
            raise IncompleteObjectException(attribute="rest type", object_type=ObjectType.QUERY_PIPE)

        if rest_type == "QueryPipe":
            return execute_query_pipe(
                forge_factory, rest["head"], new_parameters, rest["rest"], debug=debug)
        else:
            return execute_query(
                forge_factory, rest, new_parameters, last_query=True, debug=debug)


def apply_rule(forge_factory, rule, parameters, premise_check=True, debug=False):
    """Apply a rule given the input parameters.

    This function, first, checks if the premises of the rule are satisfied.
    Then runs the search query or query pipe.

    Parameters
    ----------
    forge_factory : func
        A function that takes as an input the name of the organization and
        the project, and returns a forge session.
    rule : dict
        JSON-representation of a rule
    parameters : dict, optional
        Parameter dictionary to use in premises and search queries.
    premise_check: bool, optional
    debug: bool, optional
    """
    if premise_check:
        satisfies = check_premises(forge_factory, rule, parameters, debug=debug)
    else:
        satisfies = True
    if satisfies:
        res = execute_query_pipe(
            forge_factory, rule["searchQuery"], parameters, debug=debug)
        return res
    else:
        warnings.warn(
            "Rule premise is not satisfied on the input parameters",
            InferenceToolsWarning)


def get_premise_parameters(rule):
    if "premise" not in rule:
        return {}

    premises = _enforce_list(rule["premise"])

    premise_params = {}

    for premise in premises:
        if "hasParameter" in premise:
            params = _enforce_list(premise["hasParameter"])
            premise_i_params = dict([(p["name"], p) for p in params])
            premise_params.update(premise_i_params)

    return premise_params


def get_query_pipe_params(query):
    def _get_input_params(input_params, prev_output_params=None):

        if prev_output_params is None:
            prev_output_params = []

        new_params = _enforce_list(input_params)

        return dict([(p["name"], p) for p in new_params if p["name"] not in prev_output_params])

    def _get_output_params(sub_query):
        param_mapping = sub_query.get("resultParameterMapping", [])
        if len(param_mapping) == 0:
            return []
        return [p["parameterName"] for p in _enforce_list(param_mapping)]

    def _get_head_rest(sub_query):
        try:
            query_type = _safe_get_type_attribute(sub_query)
        except TypeError:
            raise IncompleteObjectException(object_type=ObjectType.QUERY, attribute="type")

        if query_type == "QueryPipe":
            return sub_query["head"], sub_query["rest"]
        else:
            return sub_query, None

    input_parameters = {}
    output_parameters = []
    rest = query

    while rest is not None:
        head, rest = _get_head_rest(rest)
        input_parameters.update(_get_input_params(head.get("hasParameter", []), output_parameters))
        output_parameters += _get_output_params(head)

    return input_parameters


def get_rule_parameters(rule):
    """Get parameters of the input rule.

    Parameters
    ----------
    rule : dict
        Rule payload

    Returns
    -------
    all_params : dict
        Dictionary with all the input parameters. Keys are parameter names,
        values are full parameter payloads.
    """

    # Get premise parameters
    premise_params = get_premise_parameters(rule)

    # Get query parameters
    search_query = rule.get("searchQuery", None)

    if search_query is None:
        raise IncompleteObjectException(name=rule["name"], attribute="searchQuery", object_type=ObjectType.RULE)

    search_params = get_query_pipe_params(search_query)

    all_params = {**premise_params, **search_params}
    return all_params
