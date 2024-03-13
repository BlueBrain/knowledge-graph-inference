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

from collections import defaultdict
from typing import Callable, List, Dict, Tuple, Optional

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.similarity.embedding import Embedding
from inference_tools.datatypes.similarity.statistic import Statistic
from inference_tools.exceptions.exceptions import SimilaritySearchException
from inference_tools.datatypes.similarity.neighbor import Neighbor
from inference_tools.datatypes.query import SimilaritySearchQuery
from inference_tools.datatypes.query_configuration import SimilaritySearchQueryConfiguration
from inference_tools.exceptions.malformed_rule import MalformedSimilaritySearchQueryException
from inference_tools.similarity.queries.get_boosting_factor import get_boosting_factor_for_embedding
from inference_tools.similarity.queries.get_embedding_vector import get_embedding_vector
from inference_tools.similarity.queries.get_neighbors import get_neighbors
from inference_tools.similarity.queries.get_score_stats import get_score_stats
from inference_tools.similarity.similarity_model_result import SimilarityModelResult
from inference_tools.datatypes.parameter_specification import ParameterSpecification

SIMILARITY_MODEL_SELECT_PARAMETER_NAME = "SelectModelsParameter"
SPECIFIED_TARGET_RESOURCE_TYPE = "SpecifiedTargetResourceType"


def execute_similarity_query(
        forge_factory: Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge],
        query: SimilaritySearchQuery, parameter_values: Dict, debug: bool,
        use_resources: bool, limit: int
):
    """Execute similarity search query.

    Parameters
    ----------
    forge_factory : func
        Factory that returns a forge session given a bucket
    query : dict
        Json representation of the similarity search query (`SimilarityQuery`)
    parameter_values : dict
        Input parameters used in the similarity query
    debug: bool
    use_resources: bool
    limit: int

    Returns
    -------
    neighbors : list of resource ID
        List of similarity search results, each element is a resource ID.
    """

    target_parameter = query.search_target_parameter

    if target_parameter is None:
        raise MalformedSimilaritySearchQueryException("Target parameter is not specified")

    config: List[SimilaritySearchQueryConfiguration] = query.query_configurations

    if config is None:
        raise MalformedSimilaritySearchQueryException("No similarity search configuration provided")

    try:
        selected_models_spec: ParameterSpecification = next(
            p for p in query.parameter_specifications
            if p.name == SIMILARITY_MODEL_SELECT_PARAMETER_NAME
        )
        selected_models = selected_models_spec.get_value(parameter_values)
    except StopIteration:
        selected_models = [config_i.embedding_model_data_catalog.id for config_i in config]
        # Keep all if SIMILARITY_MODEL_SELECT_PARAMETER_NAME is not a part of the parameter
        # specification = all models should be kept

    valid_configs = [
        config_i for config_i in config
        if config_i.embedding_model_data_catalog.id in selected_models
    ]

    specified_derivation_type = parameter_values.get(SPECIFIED_TARGET_RESOURCE_TYPE, None)

    # selected model = embedding model catalog id

    if len(valid_configs) == 0:
        return []

    if len(valid_configs) == 1:
        config_i = valid_configs[0]

        if config_i.similarity_view.id is None:
            raise MalformedSimilaritySearchQueryException("Similarity search view is not defined")

        forge = config_i.use_factory(forge_factory, sub_view=None)

        _, neighbors = query_similar_resources(
            forge=forge,
            target_parameter=target_parameter,
            config=config_i,
            parameter_values=parameter_values,
            k=limit,
            result_filter=query.result_filter,
            debug=debug,
            use_resources=use_resources,
            specified_derivation_type=specified_derivation_type
        )

        return [
            SimilarityModelResult(
                id=n.entity_id,
                score=score,
                score_breakdown={config_i.embedding_model_data_catalog.id: (score, 1)}
            ).to_json()
            for score, n in neighbors
        ]

    return combine_similarity_models(
        k=limit,
        forge_factory=forge_factory,
        parameter_values=parameter_values,
        configurations=valid_configs,
        target_parameter=target_parameter,
        result_filter=query.result_filter,
        debug=debug,
        use_resources=use_resources,
        specified_derivation_type=specified_derivation_type
    )


def query_similar_resources(
        forge: KnowledgeGraphForge,
        config: SimilaritySearchQueryConfiguration,
        parameter_values,
        k: Optional[int],
        target_parameter: str,
        result_filter: Optional[str],
        debug: bool,
        use_resources: bool = False,
        specified_derivation_type: Optional[str] = None
) -> Tuple[Embedding, List[Tuple[int, Neighbor]]]:
    """Query similar resources using the similarity query.

    Parameters
    ----------
    forge : KnowledgeGraphForge
        Instance of a forge session
    config: dict or list of dict
        Query configuration containing references to the target views
        to be queried.
    parameter_values : dict
        Input parameters used in the similarity query
    k : int
        Number of nearest neighbors to query
    target_parameter: str
        The name of the input parameter that holds the id of the entity the results should be
        similar to
    result_filter: Optional[str]
        An additional elastic search query filter to apply onto the neighbor search, in string
        format
    debug: bool
    use_resources: bool
    specified_derivation_type: str
        Optional subtype of the rule's target resource type, specifying only neighbors of this
        subtype should be returned

    Returns
    -------
    result :  Tuple[Embedding, Dict[int, Neighbor]]
        The embedding vector of the resource being queried, as well as a dictionary
        with keys being scores and values being a Neighbor object holding
        the resource id that is similar

    """
    search_target = parameter_values.get(target_parameter, None)  # TODO should it be formatted ?

    if search_target is None:
        raise SimilaritySearchException(f"Target parameter value is not specified, a value for the"
                                        f"parameter {target_parameter} is necessary")

    embedding = get_embedding_vector(
        forge, search_target, debug=debug, use_resources=use_resources,
        derivation_type=config.embedding_model_data_catalog.about,
        model_name=config.embedding_model_data_catalog.name, view=config.similarity_view.id
    )

    result: List[Tuple[int, Neighbor]] = get_neighbors(
        forge=forge, vector_id=embedding.id, vector=embedding.vector,
        k=k, score_formula=config.embedding_model_data_catalog.distance,
        result_filter=result_filter, parameters=parameter_values, debug=debug,
        use_resources=use_resources,
        derivation_type=config.embedding_model_data_catalog.about,
        specified_derivation_type=specified_derivation_type,
        view=config.similarity_view.id
    )

    return embedding, result


def combine_similarity_models(
        forge_factory: Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge],
        configurations: List[SimilaritySearchQueryConfiguration],
        parameter_values: Dict, k: int, target_parameter: str,
        result_filter: Optional[str], debug: bool, use_resources: bool,
        specified_derivation_type: Optional[str] = None
) -> List[Dict]:
    """
    Perform similarity search combining several similarity models
    @param forge_factory:
    @type forge_factory: KnowledgeGraphForge
    @param configurations:
    @type configurations: List[SimilaritySearchQueryConfiguration]
    @param parameter_values:
    @type parameter_values: Dict
    @param k:
    @type k: int
    @param target_parameter:
    @type target_parameter: str
    @param result_filter:
    @type result_filter:
    @param debug:
    @type debug: bool
    @param use_resources: whether to query with the KnowledgeGraphForge instance or to make direct
    calls to Delta
    @type use_resources: bool
    @return:
    @param specified_derivation_type: Optional subtype of the rule's target resource type,
     specifying only neighbors of this subtype should be returned
    @type specified_derivation_type: str
    @rtype: List[Dict]
    """""

    # 1. Get neighbors for all models

    model_ids = [config_i.embedding_model_data_catalog.id for config_i in configurations]

    # Assume boosting factors and stats are in the same bucket as embeddings

    buckets = {(c.org, c.project) for c in configurations}

    print(len(buckets))

    forge_instances = dict(
        (f"{org}/{project}", forge_factory(org, project, None, None)) for org, project in buckets
    )

    # forge_instances = [
    #     config_i.use_factory(forge_factory, sub_view="similarity")
    #     for config_i in configurations
    # ]

    vector_neighbors_per_model: List[Tuple[Embedding, List[Tuple[int, Neighbor]]]] = [
        query_similar_resources(
            forge=forge_instances[config_i.get_bucket()], config=config_i,
            parameter_values=parameter_values, k=k, target_parameter=target_parameter,
            result_filter=result_filter, debug=debug, use_resources=use_resources,
            specified_derivation_type=specified_derivation_type
        )
        for config_i in configurations
    ]

    all_neighbors_across_models = set.union(*[
        set(n.entity_id for _, n in neighbors) for _, neighbors in vector_neighbors_per_model
    ])

    missing_neighbors_per_model = dict(
        (
            embedding,
            all_neighbors_across_models.difference(set(n.entity_id for _, n in neighbors))
        )
        for embedding, neighbors in vector_neighbors_per_model
    )

    for i, (embedding, missing_list) in enumerate(missing_neighbors_per_model.items()):
        missing_neighbors: List[Tuple[int, Neighbor]] = get_neighbors(
            forge=forge_instances[configurations[i].get_bucket()],
            vector_id=embedding.id, vector=embedding.vector,
            k=k, score_formula=configurations[i].embedding_model_data_catalog.distance,
            result_filter=result_filter, parameters=parameter_values, debug=debug,
            use_resources=use_resources,
            restricted_ids=list(missing_list),
            derivation_type=configurations[i].embedding_model_data_catalog.about,
            specified_derivation_type=specified_derivation_type,
            view=configurations[i].similarity_view.id
        )

        vector_neighbors_per_model[i][1].extend(missing_neighbors)

    # 2. Boost/Combine models

    equal_contribution = 1 / len(configurations)  # TODO change to user input model weight

    weights = dict((model_id, equal_contribution) for model_id in model_ids)

    combined_results: Dict[str, Dict] = defaultdict(dict)

    for i, config_i in enumerate(configurations):

        embedding, neighbors = vector_neighbors_per_model[i]

        # forge_statistics = config_i.use_factory(forge_factory, sub_view="statistic")
        statistic: Statistic = get_score_stats(
            forge=forge_instances[config_i.get_bucket()],
            config=config_i, boosted=config_i.boosted, use_resources=use_resources
        )

        if config_i.boosted:
            # forge_boosting = config_i.use_factory(forge_factory, sub_view="boosting")
            boosting_factor = get_boosting_factor_for_embedding(
                forge=forge_instances[config_i.get_bucket()], config=config_i, use_resources=use_resources,
                embedding_id=embedding.id
            )
            factor = boosting_factor.value
        else:
            factor = 1

        embedding_model_id = config_i.embedding_model_data_catalog.id

        for score_i, n in neighbors:
            combined_results[n.entity_id][embedding_model_id] = (
                normalize(score_i * factor, statistic.min, statistic.max),
                weights[embedding_model_id]
            )

        # weight is redundant but for confirmation score of proximity between n.entity_id and
        # queried resource for the key model

    combined_results_mean = [
        (
            entity_id,
            sum(score * weight for score, weight in score_dict.values()),
            score_dict
        )
        for entity_id, score_dict in combined_results.items()
    ]

    combined_results_mean.sort(key=lambda row: row[1], reverse=True)

    if len(combined_results_mean) > k:
        combined_results_mean = combined_results_mean[:k - 1]

    return [
        SimilarityModelResult(
            id=id_, score=score, score_breakdown=score_breakdown
        ).to_json()

        for id_, score, score_breakdown in combined_results_mean
    ]


def normalize(score: float, min_v: float, max_v: float) -> float:
    """
    Normalises a score, using min-max normalisation
    @param score: the score to normalise
    @type score: float
    @param min_v: the minimum score of proximity between all pairs within the population considered
    @type min_v: float
    @param max_v: the maximum score of proximity between all pairs within the population considered
    @type max_v: float
    @return: the normalised score
    @rtype: float
    """
    return (score - min_v) / (max_v - min_v)
