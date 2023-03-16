"""
Rule fetching
"""

from typing import List, Optional
from kgforge.core import KnowledgeGraphForge
from inference_tools.utils import _build_parameter_map
from inference_tools.type import QueryType, ParameterType
from inference_tools.query.sparql import Sparql
from inference_tools.query.elastic_search import ElasticSearch
from inference_tools.helper_functions import _to_symbol


def get_resource_type_descendants(forge, types) -> List[str]:
    """
    Gets the descendant types of a list of data types

    @param forge: the forge instance to run
    @type forge: KnowledgeGraphForge
    @param types: the types whose descendant we are looking for in the datatype hierarchy
    @type types: List[str]
    @return: a list of Resource labels that are descendants of the
    @rtype: List[str]
    """
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

    res = Sparql.execute_query(forge, query, current_parameters, debug=False)

    return [obj["label"] for obj in res]


def fetch_rules(forge_rules: KnowledgeGraphForge, forge_datamodels: KnowledgeGraphForge,
                rule_view_id: str,
                resource_types: Optional[List[str]] = None,
                resource_types_descendants: bool = True):
    """
    Get the rules using provided view, for an optional set of target resource types.

    @param forge_rules: a Forge instance tied to a bucket containing rules
    @type forge_rules: KnowledgeGraphForge
    @param forge_datamodels: a Forge instance tied to a bucket containing data models
    @type forge_datamodels: KnowledgeGraphForge
    @param rule_view_id: id of the view to use when retrieving rules
    @type rule_view_id: str
    @param resource_types: An optional parameter to only retrieve rules whose target resource
    type matches one of the provided ones
    @type resource_types: Optional[List[str]]
    @param resource_types_descendants: When fetching rules with specific target data types, whether
    the rule can also target parents of this data types (be more general but still applicable)
    @type resource_types_descendants: bool
    @return: a list of rules that have the specified resource_types
    @rtype: List[Dict]
    """

    old_endpoint = ElasticSearch.get_elastic_view_endpoint(forge_rules)
    ElasticSearch.set_elastic_view(forge_rules, rule_view_id)
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

        resource_type_repr = ",".join([f"\"{_to_symbol(forge_rules, t)}\"" for t in resource_types])

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

    ElasticSearch.set_elastic_view_endpoint(forge_rules, old_endpoint)

    return [
        {**forge_rules.as_json(rule), "nexus_link": rule._store_metadata._self}
        for rule in rules
    ]
