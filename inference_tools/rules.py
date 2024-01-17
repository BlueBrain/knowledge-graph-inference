"""
Rule fetching
"""
from copy import deepcopy
from string import Template
from typing import List, Optional, Dict, Union, Callable
import json
from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.parameter_specification import ParameterSpecification
from inference_tools.datatypes.query import SparqlQueryBody, SimilaritySearchQuery
from inference_tools.datatypes.query_configuration import SimilaritySearchQueryConfiguration
from inference_tools.datatypes.rule import Rule
from inference_tools.datatypes.similarity.embedding import Embedding
from inference_tools.exceptions.exceptions import SimilaritySearchException
from inference_tools.execution import check_premises
from inference_tools.helper_functions import _enforce_list
from inference_tools.nexus_utils.forge_utils import ForgeUtils
from inference_tools.parameter_formatter import ParameterFormatter
from inference_tools.similarity.main import SIMILARITY_MODEL_SELECT_PARAMETER_NAME
from inference_tools.similarity.queries.get_embeddings_vectors import get_embedding_vectors
from inference_tools.source.elastic_search import ElasticSearch
from inference_tools.type import QueryType, ParameterType, RuleType
from inference_tools.utils import get_search_query_parameters


def get_resource_type_descendants(forge, types, to_symbol=True, debug: bool = False) -> List[str]:
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

    types = list(map(lambda x: ForgeUtils.expand_uri(forge, x), types))

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
    res = forge.as_json(forge.sparql(query_body, limit=None, debug=debug))

    return [
        obj["id"] if not to_symbol else ForgeUtils.to_symbol(forge, obj["id"])
        for obj in res
    ]


def fetch_rules(
        forge_rules: KnowledgeGraphForge,
        resource_types: Optional[List[str]] = None,
        resource_types_descendants: bool = True,
        resource_ids: Optional[Union[str, List[str]]] = None,
        rule_types: Optional[List[RuleType]] = None,
        input_filters: Optional[Dict] = None,
        use_forge: bool = False,
        forge_factory: Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge] =
        None,
        debug: bool = False

) -> Union[List[Rule], Dict[str, List[Rule]]]:
    """
    Get rules. Rules can be filtered by
    - target resource types: getting rules that return only entities of specified types. If
    resource type descendants is enabled, rules that target parent types of the specified type will
    also be returned.
    - rule types: getting rules of specific rule types
    - resource ids: getting rules that can be used with the specified resources. For each
    resource, a list of rule will be applicable

    @param forge_rules: a Forge instance tied to a bucket containing rules
    @type forge_rules: KnowledgeGraphForge
    @param resource_types: An optional parameter to only retrieve rules whose target resource
    type matches one of the provided ones
    @type resource_types: Optional[List[str]]
    @param resource_types_descendants: When fetching rules with specific target data types, whether
    the rule can also target parents of this data types (be more general but still applicable)
    @type resource_types_descendants: bool
    @param resource_ids: resource ids to filter similarity search based rules, based on whether
     an embedding is available for these resources or not
    @type resource_ids: Optional[Union[str, List[str]]]
    @param rule_types: the rule types to filter by
    @type rule_types: Optional[List[RuleType]]
    @param use_forge:
    @type use_forge: bool

    @return: a list of rules if no resource ids were specified, a dictionary of list of rules if
    resource ids were specified. This dictionary's index are the resource ids.
    @rtype: Union[List[Rule], Dict[str, List[Rule]]]
    """

    rule_types = [RuleType.DataGeneralizationRule.value] \
        if rule_types is None or len(rule_types) == 0 \
        else [e.value for e in rule_types]

    q = {
        "size": ElasticSearch.NO_LIMIT,
        'query': {
            'bool': {
                'filter': [
                    {'terms': {'@type': rule_types}}
                ],
                'must': [
                    {'match': {'_deprecated': False}}
                ]
            }
        }
    }

    if resource_types is not None:

        if resource_types_descendants:
            resource_types = get_resource_type_descendants(
                forge_rules, resource_types, debug=debug)

        q["query"]["bool"]["must"].append(
            {"terms": {"targetResourceType": resource_types}}
        )

    rules = forge_rules.elastic(json.dumps(q), debug=debug)

    rules = [
        Rule({**forge_rules.as_json(r), "nexus_link": r._store_metadata._self})
        for r in rules
    ]

    if input_filters is not None:
        rules = [
            r for r in rules if check_premises(
                forge_factory=forge_factory,
                rule=r,
                parameter_values=input_filters
            )
        ]

    if resource_ids is None:
        def rule_format(rule_obj: Rule) -> Rule:

            if isinstance(rule_obj.search_query, SimilaritySearchQuery):
                rule_obj.search_query.parameter_specifications = _update_parameter_specifications(
                    rule_obj.search_query.parameter_specifications,
                    rule_obj.search_query.query_configurations
                )

            rule_obj.flattened_input_parameters = list(get_search_query_parameters(
                rule_obj).values())

            return rule_obj

        return [rule_format(r) for r in rules]
    else:
        resource_ids = _enforce_list(resource_ids)

        # list -> per rule, dict: value is rule (or partial) if relevant else None
        rule_check_per_res_id: List[Dict[str, Optional[Rule]]] = [
            rule_has_resource_ids_embeddings(
                rule, resource_ids, forge_rules, use_forge=use_forge, debug=debug
            )
            for rule in rules
        ]

        final_dict: Dict[str, List[Rule]] = dict(
            (
                res_id,
                list(
                    e
                    for e in map(lambda dict_rule: dict_rule[res_id], rule_check_per_res_id)
                    if e is not None
                )
            )
            for res_id in resource_ids
        )

        return final_dict


def rule_has_resource_ids_embeddings(
        rule: Rule, resource_ids: List[str], forge: KnowledgeGraphForge, use_forge: bool,
        debug: bool
) -> Dict[str, Optional[Rule]]:
    """
    Checks whether a rule is relevant for a list of resource ids.
    @param rule: the rule
    @type rule: Rule
    @param resource_ids: the list of resource ids
    @type resource_ids: List[str]
    @param forge: a forge instance to query for the embeddings that will indicate if the rule is
    relevant for a resource or not.
    @type forge: KnowledgeGraphForge
    @return: If a rule's search query is not a SimilaritySearchQuery, the rule is relevant for
    all resource ids.
    If the rule's search query is a SimilaritySearchQuery, for each resource id,
    we look for whether the associated resource has been embedded by the models contained in the
    rule's query configurations.
    If only some models are relevant for a resource, a partial version of the
    rule (with some query configurations filtered out) is returned for a resource id.
    If all models are relevant for a resource, the whole rule is returned for a resource id.
    If none of the models are relevant, None is returned for a resource id.
    @rtype: Dict[str, Rule]
    """

    if not isinstance(rule.search_query, SimilaritySearchQuery):
        return dict((res_id, rule) for res_id in resource_ids)

    has_embedding_dict_list: List[Dict[str, bool]] = [
        has_embedding_dict(qc, resource_ids, forge, use_forge=use_forge, debug=debug)
        for qc in rule.search_query.query_configurations
    ]

    def _handle_resource_id(res_id) -> Optional[Rule]:
        query_confs = list(
            e
            for e in map(
                lambda i_qc_res: rule.search_query.query_configurations[i_qc_res[0]]
                if i_qc_res[1][res_id] else None, enumerate(has_embedding_dict_list)
            )
            if e is not None
        )

        if len(query_confs) == 0:
            return None

        rule_i = deepcopy(rule)
        rule_i.search_query.query_configurations = query_confs
        rule_i.search_query.parameter_specifications = _update_parameter_specifications(
            rule_i.search_query.parameter_specifications, query_confs
        )
        rule_i.flattened_input_parameters = list(get_search_query_parameters(rule_i).values())
        return rule_i

    return dict((res_id, _handle_resource_id(res_id)) for res_id in resource_ids)


def _update_parameter_specifications(
        parameter_specifications: List[ParameterSpecification],
        query_configurations: List[SimilaritySearchQueryConfiguration]
):

    pos_select = next(
        i for i, e in enumerate(parameter_specifications)
        if e.name == SIMILARITY_MODEL_SELECT_PARAMETER_NAME
    )

    valid_select_values = dict(
        (qc.embedding_model_data_catalog.id, qc) for qc in query_configurations
    )

    parameter_specifications[pos_select].values = dict(
        (key, valid_select_values[value].embedding_model_data_catalog)
        for key, value in parameter_specifications[pos_select].values.items()
        if value in valid_select_values.keys()
    )

    return parameter_specifications


def has_embedding_dict(
        query_conf: SimilaritySearchQueryConfiguration,
        resource_ids: List[str],
        forge: KnowledgeGraphForge,
        use_forge: bool,
        debug: bool
) -> Dict[str, bool]:
    """
    For each resource id, checks whether it has been embedded by the model associated with the
    input query configuration. The information is returned a dictionary where the key is the
    resource id being checked and the value is a boolean indicating whether
    an embedding was found or not.
    @param query_conf: the query configuration containing information about the embedding model
    @type query_conf: SimilaritySearchQueryConfiguration
    @param resource_ids: a list of resource ids
    @type resource_ids: List[str]
    @param forge: a forge instance in order to query for embeddings
    @type forge: KnowledgeGraphForge
    @return: a dictionary indexed by the
    @rtype: Dict[str, bool]
    """
    es_view = query_conf.similarity_view.id

    endpoint, _, _ = ForgeUtils.get_endpoint_org_project(forge)

    es_endpoint = ForgeUtils.make_elastic_search_endpoint(
        org=query_conf.org, project=query_conf.project, endpoint=endpoint, view=es_view
    )

    try:
        embs: List[Embedding] = get_embedding_vectors(
            forge=forge, search_targets=resource_ids,
            use_forge=use_forge, debug=debug,
            es_endpoint=es_endpoint,
            derivation_type=query_conf.embedding_model_data_catalog.about
        )

        emb_dict: Dict[str, Optional[Embedding]] = dict((e.derivation_id, e) for e in embs)

        return dict((res_id, res_id in emb_dict) for res_id in resource_ids)

    except SimilaritySearchException:
        return dict((res_id, False) for res_id in resource_ids)
