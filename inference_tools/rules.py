"""
Rule fetching
"""
from string import Template
from typing import List, Optional, Dict
import json
import os
from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.query import SparqlQueryBody, SimilaritySearchQuery
from inference_tools.datatypes.query_configuration import SimilaritySearchQueryConfiguration
from inference_tools.datatypes.rule import Rule
from inference_tools.exceptions.exceptions import SimilaritySearchException
from inference_tools.nexus_utils.forge_utils import ForgeUtils
from inference_tools.parameter_formatter import ParameterFormatter
from inference_tools.similarity.main import SIMILARITY_MODEL_SELECT_PARAMETER_NAME
from inference_tools.similarity.queries.get_embedding_vector import get_embedding_vector
from inference_tools.source.elastic_search import ElasticSearch
from inference_tools.type import QueryType, ParameterType
from inference_tools.helper_functions import _to_symbol, _expand_uri


def get_resource_type_descendants(forge, types, to_symbol=True) -> List[str]:
    """
    Gets the descendant types of a list of data types

    @param to_symbol:
    @type to_symbol: bool
    @param forge: the forge instance to run
    @type forge: KnowledgeGraphForge
    @param types: the types whose descendant we are looking for in the datatype hierarchy
    @type types: List[str]
    @return: a list of Resource labels that are descendants of the
    @rtype: List[str]
    """

    types = list(map(lambda x: _expand_uri(forge, x), types))

    query = SparqlQueryBody({"query_string": """
            SELECT ?id ?label
            WHERE {
                ?type rdfs:subClassOf* ?id .
                ?id rdfs:label ?label
                VALUES (?type) { $types }
            }
        """})

    types = ParameterFormatter.format_parameter(
        parameter_type=ParameterType.SPARQL_VALUE_URI_LIST,
        provided_value=types, query_type=QueryType.SPARQL_QUERY, forge=forge
    )

    query_body = Template(query.query_string).substitute(types=types)
    res = forge.as_json(forge.sparql(query_body, limit=None, debug=False))

    res_label = [obj["label"] for obj in res]

    return res_label if not to_symbol else list(map(lambda x: _to_symbol(forge, x), res_label))


def fetch_rules(
        forge_rules: KnowledgeGraphForge,
        resource_types: Optional[List[str]] = None,
        resource_types_descendants: bool = True,
        resource_id: Optional[str] = None
) -> List[Rule]:
    """
    Get the rules using provided view, for an optional set of target resource types.

    @param forge_rules: a Forge instance tied to a bucket containing rules
    @type forge_rules: KnowledgeGraphForge
    @param resource_types: An optional parameter to only retrieve rules whose target resource
    type matches one of the provided ones
    @type resource_types: Optional[List[str]]
    @param resource_types_descendants: When fetching rules with specific target data types, whether
    the rule can also target parents of this data types (be more general but still applicable)
    @type resource_types_descendants: bool
    @param resource_id: str
    @param resource_id: a resource id to filter similarity search based rules, based on whether
     an embedding is available for that resource or not
    @return: a list of rules that have the specified resource_types
    @rtype: List[Dict]
    """

    q = {
        "size": ElasticSearch.NO_LIMIT,
        'query': {
            'bool': {
                'filter': [
                    {'term': {'@type': "DataGeneralizationRule"}}
                ],
                'must': [
                    {'match': {'_deprecated': False}}
                ]
            }
        }
    }

    if resource_types is not None:

        if resource_types_descendants:
            resource_types = get_resource_type_descendants(forge_rules, resource_types)

        q["query"]["bool"]["must"].append(
            {"terms": {"targetResourceType": resource_types}}
        )

    rules = forge_rules.elastic(json.dumps(q))

    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "rule_ignore.txt")) as f:
        ignored_rules = [l.strip() for l in f.readlines()]

    rules_with_ignored = [
        Rule({**forge_rules.as_json(rule), "nexus_link": rule._store_metadata._self})
        for rule in rules if rule.id not in ignored_rules
    ]

    if resource_id is None:
        return rules_with_ignored

    return [
        rule
        for rule in map(
            lambda x: rule_has_resource_id_embeddings(x, resource_id, forge_rules),
            rules_with_ignored
        )
        if rule is not None
    ]


def rule_has_resource_id_embeddings(
        rule: Rule, resource_id: str, forge: KnowledgeGraphForge
) -> Optional[Rule]:
    """
    @param rule: the rule to check
    @type rule: Rule
    @param resource_id: the entity id for which we check that the rule has embeddings for
    @type resource_id: str
    @param forge: a forge instance
    @type forge: KnowledgeGraphForge
    @return:
    - the whole rule if the rule is does not have its search query as a similarity search query
    - a partial version of the rule if its search query is a similarity search query, and only
    some of the models it uses have embeddings for the resource id
    - None if the rule search query is a similarity search query and none of the models it uses
    have embeddings for the resource id
    @rtype: Optional[Rule]
    """

    if not isinstance(rule.search_query, SimilaritySearchQuery):
        return rule

    query_confs: List[SimilaritySearchQueryConfiguration] = [
        qc
        for qc in map(lambda x: _filter_query_conf(x, resource_id, forge),
                      rule.search_query.query_configurations)
        if qc is not None
    ]

    if len(query_confs) == 0:
        return None

    pos_select = next(
        i for i, e in enumerate(rule.search_query.parameter_specifications)
        if e.name == SIMILARITY_MODEL_SELECT_PARAMETER_NAME
    )

    valid_select_values = [
        qc.embedding_model_data_catalog.id for qc in query_confs
    ]

    rule.search_query.parameter_specifications[pos_select].values = dict(
        (key, value)
        for key, value in rule.search_query.parameter_specifications[pos_select].values.items()
        if value in valid_select_values
    )

    rule.search_query.query_configurations = query_confs
    return rule


def _filter_query_conf(
        query_conf: SimilaritySearchQueryConfiguration, resource_id: str,
        forge: KnowledgeGraphForge
) -> Optional[SimilaritySearchQueryConfiguration]:
    es_view = query_conf.similarity_view.id

    endpoint, _, _ = ForgeUtils.get_endpoint_org_project(forge)

    es_endpoint = ForgeUtils.make_elastic_search_endpoint(
        org=query_conf.org, project=query_conf.project, endpoint=endpoint, view=es_view
    )

    try:
        emb = get_embedding_vector(
            forge=forge, search_target=resource_id,
            use_forge=False, debug=False, es_endpoint=es_endpoint
        )
        return query_conf
    except SimilaritySearchException:
        return None
