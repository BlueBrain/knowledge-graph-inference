"""Collection of utils for performing various inference queries."""
import json
import warnings

from string import Template

from inference_tools.similarity.utils import execute_similarity_query
from inference_tools.query.sparql import (set_sparql_view,
                                          check_sparql_premise,
                                          execute_sparql_query)
from inference_tools.query.elastic_search import (set_elastic_view,
                                                  execute_es_query)
from inference_tools.exceptions import (InferenceToolsWarning,
                                        InferenceToolsException,
                                        InvalidValueException,
                                        IncompleteObjectException,
                                        InvalidParameterTypeException,
                                        UnsupportedTypeException,
                                        ObjectType
                                        )

from inference_tools.helper_functions import _enforce_unique, _enforce_list, _safe_get_type, _follow_path

from inference_tools.type import (ParameterType, QueryType, PremiseType, format_parameter)

from kgforge.core import KnowledgeGraphForge


import getpass

DEFAULT_SPARQL_VIEW = "https://bluebrain.github.io/nexus/vocabulary/defaultSparqlIndex"
DEFAULT_ES_VIEW = "https://bluebrain.github.io/nexus/vocabulary/defaultElasticSearchIndex"


def _restore_default_views(forge):
    forge = _enforce_list(forge)
    for f in forge:
        set_sparql_view(f, DEFAULT_SPARQL_VIEW)
        set_elastic_view(f, DEFAULT_ES_VIEW)


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

def has_multi_predicate_object_pairs(parameter_spec, parameter_values):
    """
    Checks whether within the rule parameters (parameter specification),
    a parameter of type MultiPredicateObjectPair exists.

    @param parameter_spec: the parameter specification of a rule
    @param parameter_values: the parameter values specified by the user
    @return: If a parameter of this type exists, returns the name of the parameter,
        its index within the parameters specifications array, and
        the number of predicate-object pairs specified by the user in the parameter values
        else returns None
    """

    parameter_spec = _enforce_list(parameter_spec)

    try:
        types = [_safe_get_type(p) for p in parameter_spec]
        try:
            idx = types.index(ParameterType.MULTI_PREDICATE_OBJECT_PAIR.value)
            spec = parameter_spec[idx]
            name = spec["name"]
            nb_multi = len(parameter_values[name])

            return idx, name, nb_multi

        except ValueError:
            pass
    except TypeError:
        pass

    return None


def multi_predicate_object_pairs_parameter_rewriting(idx, parameter_spec, parameter_values):
    """
    Predicate-object pairs consist of type-value pairs (pairs of pairs) specified by the user of the rule to
     add additional properties of an entity to retrieve in a SPARQL query's WHERE statement.
    They are injected into the query, as such, each predicate and object become query parameters.
    These type-value pairs are split into:
    - an entry into the parameter specifications with
         - name: the MultiPredicateObject parameter name concatenated
        with "object" or "predicate" and the object-predicate pair index,
         - type: the type component of the type-value pair
    - an entry into the parameter values with:
        - name: the MultiPredicateObject parameter name concatenated
        with "object" or "predicate" and the object-pair pair index,
        - value: the value component of the type-value pair

    @param idx: The index in the parameter specifications of the parameter of type MultiPredicateObjectPair
    @param parameter_spec: the parameter specifications of the rule
    @param parameter_values: the parameter values as specified by the user
    @return: new parameter specifications and values, with the information specified by the user in the
    parameter values appropriately reorganized in the parameter specifications and parameter values so that future
    steps can treat these parameters the way standard ParameterType.s are being treated
    """
    spec = parameter_spec[idx]
    del parameter_spec[idx]
    name = spec["name"]
    provided_value = parameter_values[name]
    del parameter_values[name]

    for (i, pair) in enumerate(provided_value):
        ((predicate_value, predicate_type),
         (object_value, object_type)) = pair

        name_desc = ["predicate", "object"]
        values = [predicate_value, object_value]
        types = [predicate_type, object_type]

        for j in range(2):
            constructed_name = "{}_{}_{}".format(name, str(i), name_desc[j])
            parameter_spec.append({
                "type": types[j],
                "name": constructed_name
            })
            parameter_values[constructed_name] = values[j]

    return parameter_spec, parameter_values


def _build_parameter_map(forge, parameter_spec, parameter_values, query_type, multi=None):
    """Build parameter values given query specification."""

    forge = _enforce_unique(forge)

    parameter_spec = _enforce_list(parameter_spec)

    for spec in parameter_spec:
        optional = spec.get("optional")
        if not optional and spec["name"] not in parameter_values:
            raise InferenceToolsException(
                f"Parameter value '{spec['name']}' is not specified")

    param_map = {}

    if multi:
        (idx, name, nb_multi) = multi
        parameter_spec, parameter_values = multi_predicate_object_pairs_parameter_rewriting(idx, parameter_spec,
                                                                                            parameter_values)

    for p in parameter_spec:

        name = p["name"]

        try:
            parameter_type = _safe_get_type(p)
        except TypeError:
            raise IncompleteObjectException(name=name, attribute="type", object_type=ObjectType.PARAMETER)

        provided_value = parameter_values.get(name)

        if provided_value is None:
            continue

        try:
            parameter_type = ParameterType(parameter_type)

            param_map[name] = format_parameter(provided_value, parameter_type, query_type, forge)

        except ValueError:
            warnings.warn("Unknown parameter type {}"
                          .format(parameter_type), InferenceToolsWarning)

            param_map[name] = provided_value
            continue

    return param_map


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
    satisfies = True

    if "premise" not in rule:
        return satisfies

    premises = _enforce_list(rule["premise"])

    for premise in premises:

        config = premise.get("queryConfiguration", None)
        if config is None:
            raise IncompleteObjectException(object_type=ObjectType.PREMISE, attribute="queryConfiguration")

        forge = forge_factory(config['org'], config['project'])

        try:
            premise_type = _safe_get_type(premise)
        except TypeError:
            raise IncompleteObjectException(object_type=ObjectType.PREMISE, attribute="type")

        try:
            premise_type = PremiseType(premise_type)
        except ValueError:
            raise InvalidValueException(attribute="premise type", value=premise_type)

        if "hasParameter" in premise:
            try:
                current_parameters = _build_parameter_map(
                    forge, premise["hasParameter"], parameters, premise_type)
            except InferenceToolsException as e:
                warnings.warn(
                    "Premise is not satisfied, one or more parameters " +
                    f"are missing. See the following exception: {e}",
                    InferenceToolsWarning)
                satisfies = False
                break
        else:
            current_parameters = dict()

        if premise_type == PremiseType.SPARQL_PREMISE:
            custom_sparql_view = config.get("sparqlView", None)
            passed = check_sparql_premise(
                forge, premise, current_parameters, custom_sparql_view, debug=debug)
            if not passed:
                satisfies = False
                break

        elif premise_type == PremiseType.FORGE_SEARCH_PREMISE:
            target_param = premise.get("targetParameter", None)
            target_path = premise.get("targetPath", None)
            query = json.loads(
                Template(json.dumps(premise["pattern"])).substitute(
                    **current_parameters))
            resources = forge.search(query, debug=debug)

            resources = _enforce_list(resources)

            if target_param:
                if target_path:
                    matched_values = [
                        _follow_path(forge.as_json(r), target_path)
                        for r in resources
                    ]
                else:
                    matched_values = [r.id for r in resources]
                if current_parameters[target_param] not in matched_values:
                    satisfies = False
                    break
            else:
                if len(resources) == 0:
                    satisfies = False
                    break
        elif premise_type == PremiseType.ELASTIC_SEARCH_PREMISE:
            raise UnsupportedTypeException(premise_type.value, "premise type")  # TODO
        else:
            raise UnsupportedTypeException(premise_type.value, "premise type")

        _restore_default_views(forge)

    return satisfies


def multi_predicate_object_pairs_query_rewriting(name, nb_multi, query_body):
    """
    Rewrite the query in order to have the line where the parameter of name "name"
    duplicated for as many predicate-pairs there are and two parameters on each line,
    one for the predicate, one for the object, with a parameter naming following the one of
    @see multi_predicate_object_pairs_parameter_rewriting

    @param name: the name of the MultiPredicateObjectPairs parameter
    @param nb_multi: the number of predicate-object pairs, and therefore of
     duplication of the line where the parameter is located
    @param query_body: the query body where the rewriting of parameter placeholders is done
    @return: the rewritten query body
    """
    query_split = query_body.split("\n")
    to_find = "${}".format(name)
    index = next(i for i, line in enumerate(query_split) if to_find in line)
    replacement = lambda name, nb: "${}_{}_{} ${}_{}_{}".format(name, nb, "predicate", name, nb, "object")
    new_lines = [query_split[index].replace(to_find, replacement(name, i)) for i in range(nb_multi)]
    query_split[index] = "\n".join(new_lines)
    query_body = "\n".join(query_split)
    return query_body


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
    config = query.get("queryConfiguration", None)

    if config is None:
        raise IncompleteObjectException(object_type=ObjectType.QUERY, attribute="queryConfiguration")

    if isinstance(config, list):
        forge = [
            forge_factory(el["org"], el["project"])
            for el in config
        ]
    else:
        forge = forge_factory(config["org"], config["project"])

    try:
        query_type = _safe_get_type(query)
    except TypeError:
        raise IncompleteObjectException(object_type=ObjectType.QUERY, attribute="type")

    try:
        query_type = QueryType(query_type)
    except ValueError:
        raise InvalidValueException(attribute="query type", value=query_type)

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

    if query_type == QueryType.SPARQL_QUERY:
        custom_sparql_view = config.get("sparqlView", None)
        resources = execute_sparql_query(
            forge, query, current_parameters, custom_sparql_view, debug=debug)

    elif query_type == QueryType.FORGE_SEARCH_QUERY:
        query = json.loads(
            Template(json.dumps(query["pattern"])).substitute(
                **current_parameters))
        resources = forge.search(query)

    elif query_type == QueryType.SIMILARITY_QUERY:
        resources = execute_similarity_query(
            forge_factory, forge, query, current_parameters)

    elif query_type == QueryType.ELASTIC_SEARCH_QUERY:
        custom_es_view = config.get("elasticSearchView", None)
        resources = execute_es_query(
            forge, query, current_parameters, custom_es_view)
        if last_query:
            resources = [
                {"id": el["@id"] if "@id" in el else el["id"]}
                for el in resources
            ]
    else:
        raise UnsupportedTypeException(query_type.value, "query type")

    _restore_default_views(forge)
    return resources


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
            head_type = _safe_get_type(head)
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
            rest_type = _safe_get_type(rest)
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

    def _get_output_params(query):
        param_mapping = query.get("resultParameterMapping", [])
        if len(param_mapping) == 0:
            return []
        return [p["parameterName"] for p in _enforce_list(param_mapping)]

    def _get_head_rest(query):
        try:
            query_type = _safe_get_type(query)
        except TypeError:
            raise IncompleteObjectException(object_type=ObjectType.QUERY, attribute="type")

        if query_type == "QueryPipe":
            return query["head"], query["rest"]
        else:
            return query, None

    input_params = {}
    output_params = []
    rest = query

    while rest is not None:
        head, rest = _get_head_rest(rest)
        input_params.update(_get_input_params(head.get("hasParameter", []), output_params))
        output_params += _get_output_params(head)

    return input_params


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
