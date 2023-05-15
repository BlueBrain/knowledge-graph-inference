from collections import defaultdict

from typing import Dict, Callable, List

import json
from pandas import DataFrame

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.parameter_specification import ParameterSpecification
from inference_tools.datatypes.query import SimilaritySearchQuery
from inference_tools.datatypes.query_configuration import SimilaritySearchQueryConfiguration
from inference_tools.datatypes.similarity.boosting_factor import BoostingFactor
from inference_tools.datatypes.similarity.statistic import Statistic

from inference_tools.exceptions.malformed_rule import MalformedSimilaritySearchQueryException

from inference_tools.similarity.similarity_model_result import SimilarityModelResult
from inference_tools.similarity.single import query_similar_resources
from inference_tools.source.elastic_search import ElasticSearch

from inference_tools.exceptions.exceptions import SimilaritySearchException


def normalize(score, min_v, max_v):
    return (score - min_v) / (max_v - min_v)


def get_score_stats(forge, config: SimilaritySearchQueryConfiguration, boosted=False) -> Statistic:
    """Retrieve view statistics."""

    if config.statistics_view.id is None:
        raise MalformedSimilaritySearchQueryException("Statistics view is not defined")

    ElasticSearch.set_elastic_view(forge, config.statistics_view.id)

    q = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"_deprecated": False}},
                    {"term": {"boosted": boosted}}
                ]
            }
        }
    }
    statistics = forge.elastic(json.dumps(q))

    if statistics is None or len(statistics) == 0:
        raise SimilaritySearchException("No view statistics found")

    if len(statistics) > 1:
        print("Multiple statistics found, only getting the first one")

    statistics = forge.as_json(statistics[0])

    return Statistic(statistics)


def get_boosting_factors(forge, config: SimilaritySearchQueryConfiguration) \
        -> Dict[str, BoostingFactor]:
    """Retrieve boosting factors."""

    if config.boosting_view.id is None:
        raise MalformedSimilaritySearchQueryException("Boosting view is not defined")

    ElasticSearch.set_elastic_view(forge, config.boosting_view.id)

    factors = ElasticSearch.get_all_documents(forge)

    if factors is None or len(factors) == 0:
        raise SimilaritySearchException("No boosting factors found")

    boosting_factors = [BoostingFactor(el) for el in forge.as_json(factors)]

    return dict((el.entity_id, el) for el in boosting_factors)


def combine_similarity_models(forge_factory: Callable[[str, str], KnowledgeGraphForge],
                              query: SimilaritySearchQuery, parameter_values: Dict, k: int):
    """Perform similarity search combining several similarity models"""

    try:
        p_spec: ParameterSpecification = next(
            p for p in query.parameter_specifications
            if p.name == "IgnoreModelsParameter"
        )
        models_to_ignore = p_spec.get_value(parameter_values)
    except StopIteration:
        models_to_ignore = []

    valid_configs = [config_i for config_i in query.query_configurations
                     if config_i.embedding_model.id not in models_to_ignore]

    if len(valid_configs) == 0:
        return []

    # Assume boosting factors and stats are in the same bucket as embeddings
    valid_forges = [
        forge_factory(config_i.org, config_i.project)
        for config_i in valid_configs
    ]

    model_ids = [config_i.embedding_model.id for config_i in valid_configs]

    equal_contribution = 1 / len(valid_configs)  # TODO change to user input model weight
    weights = dict((model_id, equal_contribution) for model_id in model_ids)

    # Combine the results
    combined_results = defaultdict(dict)

    for config_i, forge_i in zip(valid_configs, valid_forges):

        vector_id, neighbors = query_similar_resources(
            forge_factory, forge_i, query, config_i, parameter_values, k=None
        )

        statistic = get_score_stats(forge_i, config_i, boosted=config_i.boosted)

        boosted_factors = get_boosting_factors(forge_i, config_i) if config_i.boosted else None

        # TODO temporary because some boosting factors are missing
        factor = (boosted_factors[vector_id].value if vector_id in boosted_factors else 1) \
            if config_i.boosted else 1

        for score_i, n in neighbors:
            score_i = normalize(score_i * factor, statistic.min, statistic.max)
            combined_results[n.entity_id][config_i.embedding_model.id] = \
                (score_i, weights[config_i.embedding_model.id])
            # weight is redundant but for confirmation
            # score of proximity between n.entity_id and
            # queried resource for the key model

    def get_weighted_score(score_dict):
        if not all([model_id in score_dict.keys() for model_id in model_ids]):
            # TODO If a score is not available for all models,
            #  most likely due to missing data in Nexus, should not happen
            return 0

        return sum([score * weight for score, weight in score_dict.values()])

    combined_results_mean = [
        (key, get_weighted_score(value), value)
        for key, value in combined_results.items()
    ]

    df = DataFrame(combined_results_mean).nlargest(k, columns=[1])

    return [
        SimilarityModelResult(
            id=id,
            score=score,
            score_breakdown=score_breakdown
        ).to_json()

        for id, score, score_breakdown in zip(df[0], df[1], df[2])
    ]

