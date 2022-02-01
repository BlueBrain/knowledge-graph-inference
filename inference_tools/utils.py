"""Collection of utils for performing various inference queries."""
import json
import warnings

from string import Template

from kgforge.core import KnowledgeGraphForge

from inference_tools.similarity.utils import execute_similarity_query
from inference_tools.query.sparql import (set_sparql_view,
                                          check_sparql_premise,
                                          execute_sparql_query)
from inference_tools.query.elastic import set_elastic_view
from inference_tools.exceptions import (InferenceToolsException,
                                        InferenceToolsWarning,
                                        MissingParameterException,
                                        MissingParameterWarning,
                                        PremiseException)


DEFAULT_SPARQL_VIEW = "https://bluebrain.github.io/nexus/vocabulary/defaultSparqlIndex"
DEFAULT_ES_VIEW = "https://bluebrain.github.io/nexus/vocabulary/defaultElasticSearchIndex"

FORGE_CONFIG = "../../configs/new-forge-config.yaml"
ENDPOINT = "https://staging.nexus.ocp.bbp.epfl.ch/v1"

# !!! This must move to the sessions of the service
FORGE_SESSIONS = dict()


def _restore_default_views(forge):
    if not isinstance(forge, list):
        forge = [forge]
    for f in forge:
        set_sparql_view(f, DEFAULT_SPARQL_VIEW)
        set_elastic_view(f, DEFAULT_ES_VIEW)


def _allocate_forge_session(config, token):
    global FORGE_SESSIONS
    if (config['org'], config['project']) not in FORGE_SESSIONS:
        FORGE_SESSIONS[(config['org'], config['project'])] = KnowledgeGraphForge(
            FORGE_CONFIG,
            endpoint=ENDPOINT,
            token=token,
            bucket=f"{config['org']}/{config['project']}")

    return FORGE_SESSIONS[(config['org'], config['project'])]


def _follow_path(json_resource, path):
    """Follow a path in a JSON-resource."""
    value = json_resource
    path = path.split(".")
    for el in path:
        value = value[el]
    return value


def _build_parameter_map(parameter_spec, parameter_values):
    """Build parameter values given query specification."""
    if isinstance(parameter_spec, dict):
        parameter_spec = [parameter_spec]

    for spec in parameter_spec:
        if spec["name"] not in parameter_values:
            raise MissingParameterException(
                "Parameter value '{}' is not specified".format(spec['name']))

    param_map = {}

    if isinstance(parameter_spec, dict):
        parameter_spec = [parameter_spec]

    for p in parameter_spec:
        name = p["name"]
        if p["type"] == "list":
            param_map[name] = ", ".join([
                f"\"{el}\"" for el in parameter_values[name]])
        elif p["type"] == "str":
            if isinstance(parameter_values[name], list):
                value = parameter_values[name][0]
            else:
                value = parameter_values[name]
            param_map[name] = f"\"{value}\""
        elif p["type"] == "sparql_uri":
            if isinstance(parameter_values[name], list):
                value = parameter_values[name][0]
            else:
                value = parameter_values[name]

            param_map[name] = value
        else:
            param_map[name] = parameter_values[name]
    return param_map


def check_premises(rule, parameters, token):
    """Check if the rule premises are satisfied given the parameters.

    Parameters
    ----------
    forge : KnowledgeGraphForge
        Instance of a forge session
    rule : dict
        JSON-representation of a rule
    parameters : dict, optional
        Parameter dictionary to use in the premise queries.

    Returns
    -------
    satisfies : bool
    """
    satisfies = True

    if isinstance(rule["premise"], dict):
        rule["premise"] = [rule["premise"]]

    for premise in rule["premise"]:
        config = premise.get("queryConfiguration", None)
        if config is None:
            raise PremiseException(
                "Query configuration is not provided")

        forge = _allocate_forge_session(config, token)

        current_parameters = dict()
        if "hasParameter" in premise:
            try:
                current_parameters = _build_parameter_map(
                    premise["hasParameter"], parameters)
            except MissingParameterException as e:
                warnings.warn(
                    "Premise is not satified, one or more parameters " +
                    f"are missing. See the following exception: {e}",
                    MissingParameterWarning)
                satisfies = False
                break

        if premise["type"] == "SparqlPremise":
            custom_sparql_view = config.get("sparqlView", None)
            passed = check_sparql_premise(
                forge, query, parameters, custom_sparql_view)
            if not passed:
                satisfies = False
                break
        elif premise["type"] == "ForgeSearchPremise":
            target_param = premise.get("targetParameter", None)
            target_path = premise.get("targetPath", None)
            query = json.loads(
                Template(json.dumps(premise["pattern"])).substitute(
                    **current_parameters))
            resources = forge.search(query)
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
        elif premise["type"] == "ElasticSearchPremise":
            print(premise)
        else:
            raise InferenceToolsException("Unknown type of premise")

        _restore_default_views(forge)

    return satisfies


def execute_query(query, parameters, token):
    """Execute an individual query given parameters.

    Parameters
    ----------
    query : dict
        JSON-representation of a query
    parameters : dict, optional
        Parameter dictionary to use in the query.

    Returns
    -------
    resources : list
        List of the result resources
    """
    config = query.get("queryConfiguration", None)
    if config is None:
        raise QueryException(
            "Query configuration is not provided")
    if isinstance(config, list):
        forge = [
            _allocate_forge_session(el, token)
            for el in config
        ]
    else:
        forge = _allocate_forge_session(config, token)

    current_parameters = dict()
    if "hasParameter" in query:
        try:
            current_parameters = _build_parameter_map(
                query["hasParameter"], parameters)
        except MissingParameterException as e:
            raise QueryException(
                "Query cannot be executed, one or more parameters " +
                f"are missing. See the following exception: {e}")

    resources = None
    if query["type"] == "SparqlQuery":
        custom_sparql_view = config.get("sparqlView", None)
        resources = execute_sparql_query(
            forge, query, parameters, custom_sparql_view)
    elif query["type"] == "ForgeSearchQuery":
        query = json.loads(
            Template(json.dumps(query["pattern"])).substitute(
                **current_parameters))
        resources = forge.search(query)
    elif query["type"] == "SimilarityQuery":
        resources = execute_similarity_query(forge, query, current_parameters)
    elif query["type"] == "ElasticSearchQuery":
        query = Template(query["hasBody"]).substitute(**current_parameters)
        resources = forge.as_json(forge.elastic(query, limit=None))
    else:
        raise InferenceToolsException("Unknown type of query")
    _restore_default_views(forge)
    return resources


def execute_query_pipe(head, parameters, token, rest=None):
    """Execute a query pipe given the input parameters.

    This recursive function executes pipes of queries and performs
    parameter building between each individual query.

    Parameters
    ----------
    head : dict
        JSON-representation of a head query
    parameters : dict, optional
        Input parameter dictionary to use in the queries.
    rest : dict, optional
        JSON-representation of the remaining query or query pipe
    """
    if rest is None:
        if head["type"] == "QueryPipe":
            return execute_query_pipe(
                head["head"], parameters, token, head["rest"])
        else:
            return execute_query(head, parameters, token)
    else:
        result = execute_query(head, parameters, token)
        # Compute new parameters combining old parameters and the result
        new_parameters = {**parameters}
        if isinstance(head["resultParameterMapping"], dict):
            head["resultParameterMapping"] = [head["resultParameterMapping"]]

        for mapping in head["resultParameterMapping"]:
            if isinstance(result, list):
                new_parameters[mapping["parameterName"]] = [
                    _follow_path(el, mapping["path"]) for el in result
                ]
            else:
                new_parameters[mapping["parameterName"]] =\
                    result[mapping["path"]]
        if rest["type"] == "QueryPipe":
            return execute_query_pipe(
                rest["head"], new_parameters, token, rest["rest"])
        else:
            return execute_query(rest, new_parameters, token)


def apply_rule(rule, parameters, token):
    """Apply a rule given the input parameters.

    This function, first, checks if the premises of the rule are satisfied.
    Then runs the search query or query pipe.

    Parameters
    ----------
    forge : KnowledgeGraphForge
        Instance of a forge session
    rule : dict
        JSON-representation of a rule
    parameters : dict, optional
        Parameter dictionary to use in premises and search queries.
    """
    if check_premises(rule, parameters, token):
        return execute_query_pipe(rule["searchQuery"], parameters, token)
    else:
        warnings.warn(
            "Rule premise is not satisfied on the input parameters",
            InferenceToolsWarning)
