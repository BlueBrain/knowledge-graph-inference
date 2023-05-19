"""
Rule fetching
"""
from string import Template
from typing import List, Optional, Dict
import json
import os
from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.query import SparqlQueryBody
from inference_tools.datatypes.rule import Rule
from inference_tools.nexus_utils.forge_utils import ForgeUtils
from inference_tools.parameter_formatter import ParameterFormatter
from inference_tools.type import QueryType, ParameterType
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
    query = SparqlQueryBody("""
            SELECT ?id ?label
            WHERE {
                ?type rdfs:subClassOf* ?id .
                ?id rdfs:label ?label
                VALUES (?type) { $types }
            }
        """)

    types = ParameterFormatter.format_parameter(
        parameter_type=ParameterType.SPARQL_VALUE_URI_LIST,
        provided_value=types, query_type=QueryType.SPARQL_QUERY, forge=forge
    )

    query_body = Template(query).substitute(types=types)
    res = forge.as_json(forge.sparql(query_body, limit=None, debug=False))

    return [obj["label"] for obj in res]


def fetch_rules(forge_rules: KnowledgeGraphForge, forge_datamodels: KnowledgeGraphForge,
                rule_view_id: str,
                resource_types: Optional[List[str]] = None,
                resource_types_descendants: bool = True) -> List[Rule]:
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

    old_endpoint = ForgeUtils.get_elastic_search_endpoint(forge_rules)
    ForgeUtils.set_elastic_search_view(forge_rules, rule_view_id)

    q = {
        "query": {
            "bool": {
                "filter": [{"term": {"_deprecated": False}}],
                "must": []
            }
        }
    }

    if resource_types is not None:

        if resource_types_descendants:
            resource_types = get_resource_type_descendants(forge_datamodels, resource_types)

        resource_type_repr = [_to_symbol(forge_rules, t) for t in resource_types]

        q["query"]["bool"]["must"].append(
            {"terms": {"targetResourceType": resource_type_repr}}
        )

    rules = forge_rules.elastic(json.dumps(q))
    ForgeUtils.set_elastic_search_endpoint(forge_rules, old_endpoint)

    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "rule_ignore.txt")) as f:
        ignored_rules = [l.strip() for l in f.readlines()]

    return [
        Rule({**forge_rules.as_json(rule), "nexus_link": rule._store_metadata._self})
        for rule in rules if rule.id not in ignored_rules
    ]
