"""Collection of utils for performing various inference queries."""
import json
import warnings

from string import Template

from inference_tools.similarity.utils import execute_similarity_query
from inference_tools.query.sparql import (set_sparql_view,
                                          check_sparql_premise,
                                          execute_sparql_query)
from inference_tools.query.elastic_search import (set_elastic_view,
                                                  get_elastic_view_endpoint,
                                                  set_elastic_view_endpoint)
from inference_tools.exceptions import (InferenceToolsException,
                                        InferenceToolsWarning,
                                        MissingParameterException,
                                        MissingParameterWarning,
                                        PremiseException,
                                        QueryException,
                                        QueryTypeException)


DEFAULT_SPARQL_VIEW = "https://bluebrain.github.io/nexus/vocabulary/defaultSparqlIndex"
DEFAULT_ES_VIEW = "https://bluebrain.github.io/nexus/vocabulary/defaultElasticSearchIndex"


def _safe_get_type(query):
    query_type = (
        query.get("type", None)
        if query.get("type", None)
        else query.get("@type")
    )
    if query_type is None:
        raise TypeError(
            "Unknown entity type")
    return query_type


def _restore_default_views(forge):
    if not isinstance(forge, list):
        forge = [forge]
    for f in forge:
        set_sparql_view(f, DEFAULT_SPARQL_VIEW)
        set_elastic_view(f, DEFAULT_ES_VIEW)


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

        try:
            parameter_type = _safe_get_type(p)
        except TypeError:
            raise QueryTypeException("Unknown parameter type")

        if parameter_type == "list":
            param_map[name] = ", ".join([
                f"\"{el}\"" for el in parameter_values[name]])
        elif parameter_type == "str":
            if isinstance(parameter_values[name], list):
                value = parameter_values[name][0]
            else:
                value = parameter_values[name]
            param_map[name] = f"\"{value}\""
        elif parameter_type == "sparql_uri":
            if isinstance(parameter_values[name], list):
                value = parameter_values[name][0]
            else:
                value = parameter_values[name]
            param_map[name] = value
        else:
            param_map[name] = parameter_values[name]
    return param_map


def check_premises(forge_factory, rule, parameters):
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
        forge = forge_factory(config['org'], config['project'])
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

        try:
            premise_type = _safe_get_type(premise)
        except TypeError:
            raise("Unknown premise type")

        if premise_type == "SparqlPremise":
            custom_sparql_view = config.get("sparqlView", None)
            passed = check_sparql_premise(
                forge, premise, parameters, custom_sparql_view)
            if not passed:
                satisfies = False
                break
        elif premise_type == "ForgeSearchPremise":
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
        elif premise_type == "ElasticSearchPremise":
            print(premise)
        else:
            raise PremiseException("Unknown type of premise")

        _restore_default_views(forge)

    return satisfies


def execute_query(forge_factory, query, parameters):
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
            forge_factory(el["org"], el["project"])
            for el in config
        ]
    else:
        forge = forge_factory(config["org"], config["project"])

    current_parameters = dict()
    if "hasParameter" in query:
        try:
            current_parameters = _build_parameter_map(
                query["hasParameter"], parameters)
        except MissingParameterException as e:
            raise QueryException(
                "Query cannot be executed, one or more parameters " +
                f"are missing. See the following exception: {e}")

    try:
        query_type = _safe_get_type(query)
    except TypeError:
        raise QueryTypeException("Unknown query type")

    resources = None
    if query_type == "SparqlQuery":
        custom_sparql_view = config.get("sparqlView", None)
        resources = execute_sparql_query(
            forge, query, current_parameters, custom_sparql_view)
    elif query_type == "ForgeSearchQuery":
        query = json.loads(
            Template(json.dumps(query["pattern"])).substitute(
                **current_parameters))
        resources = forge.search(query)
    elif query_type == "SimilarityQuery":
        resources = execute_similarity_query(forge, query, current_parameters)
    elif query_type == "ElasticSearchQuery":
        query = Template(query["hasBody"]).substitute(**current_parameters)
        resources = forge.as_json(forge.elastic(query, limit=None))
    else:
        raise InferenceToolsException("Unknown type of query")
    _restore_default_views(forge)
    return resources


def execute_query_pipe(forge_factory, head, parameters, rest=None):
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
    """
    if rest is None:
        try:
            head_type = _safe_get_type(head)
        except TypeError:
            raise QueryException(
                "Invalid query pipe: unknown query type of the head")

        if head_type == "QueryPipe":
            return execute_query_pipe(
                forge_factory, head["head"], parameters, head["rest"])
        else:
            return execute_query(forge_factory, head, parameters)
    else:
        result = execute_query(forge_factory, head, parameters)
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

        try:
            rest_type = _safe_get_type(rest)
        except TypeError:
            raise QueryException(
                "Invalid query pipe: unknown query type of the rest")

        if rest_type == "QueryPipe":
            return execute_query_pipe(
                forge_factory, rest["head"], new_parameters, rest["rest"])
        else:
            return execute_query(forge_factory, rest, new_parameters)


def apply_rule(forge_factory, rule, parameters, premise_check=True):
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
    """
    if premise_check:
        satisfies = check_premises(forge_factory, rule, parameters)
    else:
        satisfies = True
    if satisfies:
        res = execute_query_pipe(
            forge_factory, rule["searchQuery"], parameters)
        return res
    else:
        warnings.warn(
            "Rule premise is not satisfied on the input parameters",
            InferenceToolsWarning)


def fetch_rules(forge, rule_view_id, resource_types=None):
    """Get all the rules using provided view.

    Parameters
    ----------
    forge : KnowledgeGraphForge
        Instance of a forge session
    rule_view_id : str
        Id of the view to use when retrieving rules
    resource_types : list, optional
        List of resource types to fetch the rules for

    Returns
    -------
    rules : list of dict
        Result rule payloads
    """
    old_endpoint = get_elastic_view_endpoint(forge)
    set_elastic_view(forge, rule_view_id)
    if resource_types is None:
        rules = forge.elastic("""
            {
              "query": {
                "term": {
                  "_deprecated": false
                }
              }
            }
        """)
    else:
        resource_type_repr = "".join([f"\"{t}\"" for t in resource_types])
        rules = forge.elastic(f"""{{
          "query": {{
            "bool": {{
                "must": [
                    {{
                       "terms": {{"targetResourceType": [{resource_type_repr}]}}
                    }},
                    {{
                        "term": {{"_deprecated": false}}
                    }}
                ]
             }}
           }}
        }}""")

    set_elastic_view_endpoint(forge, old_endpoint)

    rules = forge.as_json(rules)
    return rules


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
    def _add_input_params(input_params, all_params, prev_output_params=None):
        if prev_output_params is None:
            prev_output_params = []

        new_params = (
            input_params
            if isinstance(input_params, list)
            else [input_params]
        )
        for p in new_params:
            if p["name"] not in prev_output_params:
                all_params[p["name"]] = p
 
    def _get_output_params(query):
        param_mapping = query.get("resultParameterMapping", [])
        result_params = (
            param_mapping
            if isinstance(param_mapping, list)
            else [param_mapping]
        )
        return [p["parameterName"] for p in result_params]
    
    def _get_head_rest(query):
        head = None
        rest = None

        try:
            query_type = _safe_get_type(query)
        except TypeError:
            raise QueryException(
                "Unknown query type")

        if query_type == "QueryPipe":
            head = query["head"]
            rest = query["rest"]
        else:
            head = query
        return head, rest
    
    def _get_pipe_params(query):
        params = {}
        
        head, rest = _get_head_rest(query)
        _add_input_params(head.get("hasParameter", []), params)
        output_params = _get_output_params(head)

        while rest is not None:
            head, rest = _get_head_rest(rest)
            _add_input_params(
                head.get("hasParameter", []), params, output_params)
            output_params = _get_output_params(head)

        return params

    # Get premise parameters
    premise_params = {}
    if isinstance(rule["premise"], dict):
        rule["premise"] = [rule["premise"]]
    for premise in rule["premise"]:
        params = (
            premise["hasParameter"]
            if isinstance(premise["hasParameter"], list)
            else [premise["hasParameter"]]
        )
        for p in params:
            premise_params[p["name"]] = p

    # Get query parameters
    search_params = _get_pipe_params(rule["searchQuery"])

    all_params = {**premise_params, **search_params}
    return all_params
