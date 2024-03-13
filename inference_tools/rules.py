# This file is part of knowledge-graph-inference.
# Copyright 2024 Blue Brain Project / EPFL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Rule fetching
"""
from copy import deepcopy
from string import Template
from typing import List, Optional, Dict, Union, Callable, Any
import json
from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.parameter_specification import ParameterSpecification
from inference_tools.datatypes.query import SparqlQueryBody, SimilaritySearchQuery
from inference_tools.datatypes.query_configuration import SimilaritySearchQueryConfiguration
from inference_tools.datatypes.rule import Rule
from inference_tools.datatypes.similarity.embedding import Embedding
from inference_tools.exceptions.exceptions import SimilaritySearchException, InferenceToolsException
from inference_tools.exceptions.malformed_rule import InvalidParameterSpecificationException
from inference_tools.execution import check_premises
from inference_tools.helper_functions import _enforce_list
from inference_tools.nexus_utils.forge_utils import ForgeUtils
from inference_tools.parameter_formatter import ParameterFormatter
from inference_tools.similarity.main import SIMILARITY_MODEL_SELECT_PARAMETER_NAME
from inference_tools.similarity.queries.get_embeddings_vectors import get_embedding_vectors
from inference_tools.source.elastic_search import ElasticSearch
from inference_tools.type import QueryType, ParameterType, RuleType
from inference_tools.utils import get_search_query_parameters


ignore_list = [
    "https://bbp.epfl.ch/neurosciencegraph/data/b5542787-b127-46ee-baa5-798d5d9a33bc",  # multiple sp
    "https://bbp.epfl.ch/neurosciencegraph/data/54c336ad-4017-4fd7-973c-f1926c4b0c16",  # multiple br
    "https://bbp.epfl.ch/neurosciencegraph/data/70e8e757-1834-420c-bcc1-37ea850ddfe3",  # location
    "https://bbp.epfl.ch/neurosciencegraph/data/ac5885c8-bb70-4336-ae7f-3e1425356fe8",  # shape
    "https://bbp.epfl.ch/neurosciencegraph/data/da2c61f5-59d9-4347-9c3a-0cdf2c093547",  # sscx
    # "https://bbp.epfl.ch/neurosciencegraph/data/9d64dc0d-07d1-4624-b409-cdc47ccda212" # BR bbp/ont
]


def get_resource_type_descendants(forge, types, to_symbol=True, debug: bool = False) -> List[str]:
    """
    Gets the descendant types of a list of data types

    @param to_symbol: Whether to return the type descendants as symbols or full iris
    @type to_symbol: bool
    @param forge: the forge instance to run
    @type forge: KnowledgeGraphForge
    @param types: the types whose descendant we are looking for in the datatype hierarchy
    @type types: List[str]
    @param debug: Whether to print the queries being executed or not
    @type debug: bool
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


def rule_format_basic(rule_obj: Rule) -> Rule:
    """
    Applies formatting on a rule
    - Within the parameter specifications (used by the client),
    add additional data related to the embedding models: using the reference to the
    embedding model data catalogs found in the parameter specification,
    this goes through the embedding models listed in the search query query configurations,
    and picks some fields of the embedding models to expose to the client by
    adding them to the parameter specification
    - The input parameters are flattened in the case of query pipes and set to the rule's
    flattened_input_parameters field

    @param rule_obj: the rule to format
    @type rule_obj: Rule
    @return: the formatted rule
    @rtype: Rule
    """
    if isinstance(rule_obj.search_query, SimilaritySearchQuery):
        rule_obj.search_query.parameter_specifications = _update_parameter_specifications(
            rule_obj.search_query.parameter_specifications,
            rule_obj.search_query.query_configurations
        )

    rule_obj.flattened_input_parameters = list(get_search_query_parameters(rule_obj).values())

    return rule_obj


def fetch_rules(
        forge_rules: KnowledgeGraphForge,
        resource_types: Optional[List[str]] = None,
        resource_types_descendants: bool = True,
        resource_ids: Optional[Union[str, List[str]]] = None,
        rule_types: Optional[List[RuleType]] = None,
        input_filters: Optional[Dict] = None,
        use_resources: bool = False,
        forge_factory: Optional[Callable[
            [str, str, Optional[str], Optional[str]], KnowledgeGraphForge
        ]] = None,
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
    @param forge_factory: a method to instanciate a forge instance to query for the
    embeddings that will indicate if the rule is relevant for a resource or not.
    @type forge_factory: Callable
    @param use_resources: Whether to manipulate Resource objects when getting back ElasticSearch
    results or not
    @type use_resources: bool
    @param debug: Whether to print the queries being executed or not
    @type debug: bool
    @param input_filters: filters to run against rule premises
    @type input_filters: Optional[Dict]
    @return: a list of rules if no resource ids were specified, a dictionary of list of rules if
    resource ids were specified. This dictionary's index are the resource ids.
    @rtype: Union[List[Rule], Dict[str, List[Rule]]]
    """
    # Rule filter by type: default filter or provided
    rule_types_str = [RuleType.DATA_GENERALIZATION_RULE.value] \
        if rule_types is None or len(rule_types) == 0 \
        else [e.value for e in rule_types]

    # Query by rule type
    q: Dict[str, Any] = {
        "size": ElasticSearch.NO_LIMIT,
        'query': {
            'bool': {
                'filter': [
                    {'terms': {'@type': rule_types_str}}
                ],
                'must': [
                    {'match': {'_deprecated': False}}
                ]
            }
        }
    }

    # Add target resource type to query
    if resource_types is not None:

        # add the target resource type and its descendant types
        if resource_types_descendants:
            resource_types = get_resource_type_descendants(forge_rules, resource_types, debug=debug)

        q["query"]["bool"]["must"].append(
            {"terms": {"targetResourceType": resource_types}}
        )

    rules = forge_rules.elastic(json.dumps(q), debug=debug)

    # Turn rules to Rule instances
    rules = [
        Rule({**forge_rules.as_json(r), "nexus_link": r._store_metadata._self})
        for r in rules
    ]

    # Ignore some hardcoded rules
    rules = [r for r in rules if r.id not in ignore_list]

    # Check premises of rules if some input filters were provided
    if input_filters is not None:

        if forge_factory is None:
            raise InferenceToolsException("Cannot check premises without a forge factory specified")

        rules = [
            r for r in rules if check_premises(
                forge_factory=forge_factory,
                rule=r,
                parameter_values=input_filters
            )
        ]

    # If no resource id is provided, apply basic formatting on rules
    if resource_ids is None:
        return [rule_format_basic(r) for r in rules]

    resource_id_list: List[str] = _enforce_list(resource_ids)

    # Non similarity search rules have basic formatting applied
    non_sim_formatted = [
        rule_format_basic(r) for r in rules
        if not isinstance(r.search_query, SimilaritySearchQuery)
    ]

    if forge_factory is None:
        raise SimilaritySearchException(
            "Cannot check resource id has embeddings without a forge factory specified"
        )

    # list -> per rule, dict: value is rule (or partial) if relevant else None
    rule_check_per_res_id: List[Dict[str, Optional[Rule]]] = [
        rule_has_resource_ids_embeddings(
            rule, resource_id_list, forge_factory=forge_factory, use_resources=use_resources,
            debug=debug
        )
        for rule in rules if isinstance(rule.search_query, SimilaritySearchQuery)
    ] + [
        dict((res_id, rule) for res_id in resource_ids)
        for rule in non_sim_formatted
    ]

    # Dict: key: res_id, value: List[Rule], iterate over
    final_dict: Dict[str, List[Rule]] = dict(
        (
            res_id,
            [dict_rule[res_id] for dict_rule in rule_check_per_res_id  # type: ignore
             if dict_rule[res_id] is not None]
        )
        for res_id in resource_ids
    )

    return final_dict


def rule_has_resource_ids_embeddings(
        rule: Rule, resource_ids: List[str],
        forge_factory: Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge],
        use_resources: bool, debug: bool
) -> Dict[str, Optional[Rule]]:
    """
    Checks whether a rule is relevant for a list of resource ids.
    @param rule: the rule
    @type rule: Rule
    @param resource_ids: the list of resource ids
    @type resource_ids: List[str]
    @param forge_factory: a method to instanciate a forge instance to query for the
    embeddings that will indicate if the rule is relevant for a resource or not.
    @type forge_factory: Callable
    @param use_resources: Whether to manipulate Resource objects when getting back ElasticSearch
    results or not
    @type use_resources: bool
    @param debug: Whether to print the queries being executed or not
    @type debug: bool
    @return: If a rule's search query is not a SimilaritySearchQuery, the rule is relevant for
    all resource ids.
    If the rule's search query is a SimilaritySearchQuery, for each resource id,
    we look for whether the associated resource has been embedded by the models contained in the
    rule's query configurations.
    If only some models are relevant for a resource, a partial version of the
    rule (with some query configurations filtered out) is returned for a resource id.
    If all models are relevant for a resource, the whole rule is returned for a resource id.
    If none of the models are relevant, None is returned for a resource id.
    @rtype: Dict[str, Optional[Rule]]
    """

    if not isinstance(rule.search_query, SimilaritySearchQuery):
        raise SimilaritySearchException(
            "Cannot check if rule has resource id embeddings for a rule "
            "that does not hold a similarity search query"
        )

    buckets = {(c.org, c.project) for c in rule.search_query.query_configurations}

    forge_instances = dict(
        (f"{org}/{project}", forge_factory(org, project, None, None)) for org, project in buckets
    )

    has_embedding_dict_list: List[Dict[str, bool]] = [
        has_embedding_dict(
            qc, resource_ids, forge=forge_instances[qc.get_bucket()],
            use_resources=use_resources, debug=debug
        )
        for qc in rule.search_query.query_configurations
    ]

    def _handle_resource_id(res_id) -> Optional[Rule]:
        if not isinstance(rule.search_query, SimilaritySearchQuery):
            raise SimilaritySearchException(
                "Cannot check if rule has resource id embeddings for a rule "
                "that does not hold a similarity search query"
            )

        query_confs: List[SimilaritySearchQueryConfiguration] = list(
            rule.search_query.query_configurations[i]
            for i, per_qc in enumerate(has_embedding_dict_list) if per_qc[res_id]
        )

        if len(query_confs) == 0:
            return None

        rule_i = deepcopy(rule)

        if not isinstance(rule_i.search_query, SimilaritySearchQuery):
            raise SimilaritySearchException(
                "Cannot check if rule has resource id embeddings for a rule "
                "that does not hold a similarity search query"
            )

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

    f = parameter_specifications[pos_select].values

    if f is None:
        raise InvalidParameterSpecificationException(
            f"{SIMILARITY_MODEL_SELECT_PARAMETER_NAME} should have a predefined list of values"
        )

    parameter_specifications[pos_select].values = dict(
        (key, valid_select_values[value].embedding_model_data_catalog)
        for key, value in f.items()
        if value in valid_select_values.keys()
    )

    return parameter_specifications


def has_embedding_dict(
        query_conf: SimilaritySearchQueryConfiguration,
        resource_ids: List[str],
        forge: KnowledgeGraphForge,
        use_resources: bool,
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
    @param use_resources: Whether to manipulate Resource objects when getting back ElasticSearch
    results or not
    @type use_resources: bool
    @param debug: Whether to print the queries being executed or not
    @type debug: bool
    @return: a dictionary indexed by the
    @rtype: Dict[str, bool]
    """

    try:
        embs: List[Embedding] = get_embedding_vectors(
            forge=forge, search_targets=resource_ids,
            use_resources=use_resources, debug=debug,
            view=query_conf.similarity_view.id,
            derivation_type=query_conf.embedding_model_data_catalog.about
        )

        emb_dict: Dict[str, Optional[Embedding]] = dict((e.derivation_id, e) for e in embs)

        return dict((res_id, res_id in emb_dict) for res_id in resource_ids)

    except SimilaritySearchException:
        return dict((res_id, False) for res_id in resource_ids)
