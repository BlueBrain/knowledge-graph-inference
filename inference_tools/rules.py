from inference_tools.query.elastic_search import (set_elastic_view,
                                                  get_elastic_view_endpoint,
                                                  set_elastic_view_endpoint)
from inference_tools.query.sparql import execute_sparql_query
from inference_tools.utils import _build_parameter_map
from inference_tools.type import QueryType, ParameterType


def get_resource_type_descendants(forge, types):
    query = {
        "hasBody": """ 
            SELECT ?id ?label
            WHERE {
                ?type rdfs:subClassOf* ?id .
                ?id rdfs:label ?label
                VALUES (?type) { $types } 
            }
        """}

    current_parameters = _build_parameter_map(
        forge, [
            {
                "type": ParameterType.SPARQL_VALUE_URI_LIST.value,
                "name": "types"
            }
        ], {"types": types}, QueryType.SPARQL_QUERY, multi=False)

    res = execute_sparql_query(
        forge, query, current_parameters, custom_sparql_view=None, debug=True)

    return [obj["label"] for obj in res]


def fetch_rules(forge_rules, forge_datamodels, rule_view_id, resource_types=None,
                resource_types_descendants=True):
    """Get all the rules using provided view.

    Parameters
    ----------
    forge_rules : KnowledgeGraphForge
        Instance of a forge session connected to a rules bucket
    forge_datamodels : KnowledgeGraphForge
        Instance of a forge session connected to a datamodels bucket
    rule_view_id : str
        id of the view to use when retrieving rules
    resource_types : list, optional
        List of resource types to fetch the rules for
    resource_types_descendants: bool, optional
        Whether the rule's resource type can be a parent of the queried resource types
    Returns
    -------
    rules : list of dict
        Result rule payloads
    """
    old_endpoint = get_elastic_view_endpoint(forge_rules)
    set_elastic_view(forge_rules, rule_view_id)
    if resource_types is None:
        rules = forge_rules.elastic("""
            {
              "query": {
                "term": {
                  "_deprecated": false
                }
              }
            }
        """)
    else:

        if resource_types_descendants:
            resource_types = get_resource_type_descendants(forge_datamodels, resource_types)

        resource_type_repr = ",".join([f"\"{t}\"" for t in resource_types])

        rules = forge_rules.elastic(f"""{{
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

    set_elastic_view_endpoint(forge_rules, old_endpoint)

    rules = forge_rules.as_json(rules)
    return rules
