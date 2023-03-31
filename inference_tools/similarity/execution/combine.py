import json
import math

from collections import defaultdict, namedtuple
from typing import Dict, Callable, List

import numpy as np
import pandas as pd

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.query import SimilaritySearchQuery
from inference_tools.datatypes.query_configuration import SimilaritySearchQueryConfiguration
from inference_tools.helper_functions import get_id_attribute
from inference_tools.similarity.execution.single import query_similar_resources, get_neighbors, \
    FORMULAS
from inference_tools.source.elastic_search import ElasticSearch
from inference_tools.exceptions import SimilaritySearchException


def compute_statistics(forge, view_id, score_formula, boosting=None):
    """Compute similarity score statistics given a view."""
    ElasticSearch.set_elastic_view(forge, view_id)
    all_vectors = ElasticSearch.get_all_documents(forge)
    all_vectors_json = forge.as_json(all_vectors)

    scores = []

    for vector_resource in all_vectors_json:

        vector_id = get_id_attribute(vector_resource)

        neighbors = get_neighbors(
            vector=vector_resource["embedding"],
            forge=forge,  vector_id=vector_id,
            k=len(all_vectors), score_formula=score_formula
        )
        boosting_value = boosting[vector_id] if boosting else 1

        scores += [score * boosting_value for score, _ in neighbors]

    scores = np.array(list(scores))
    Statistics = namedtuple('Statistics', 'min max mean std')

    return len(scores), Statistics(
        scores.min(), scores.max(),
        scores.mean(), scores.std())


def compute_score_deviation(forge, point_id, vector, score_min, score_max, k, formula):
    """Compute similarity score deviation for each vector."""
    query = {
        "size": k,
        "query": {
            "script_score": {
                "query": {
                    "exists": {
                        "field": "embedding"
                    }
                },
                "script": {
                    "source": FORMULAS[formula],
                    "params": {
                        "query_vector": vector
                    }
                }
            }
        }
    }

    result = forge.elastic(query)
    scores = set()
    for el in result:
        if point_id != get_id_attribute(forge.as_json([el])[0]):
            # Min/max normalization of the score
            score = (el._store_metadata._score - score_min) / (
                    score_max - score_min)
            scores.add(score)

    scores = np.array(list(scores))

    return math.sqrt(((1 - scores) ** 2).mean())


def compute_boosting_factors(forge, view_id, stats, formula,
                             neighborhood_size=10):
    """Compute boosting factors for all vectors."""
    boosting_factors = {}
    # Compute local similarity deviations for points
    ElasticSearch.set_elastic_view(forge, view_id)
    all_vectors = ElasticSearch.get_all_documents(forge)

    for vector_resource in all_vectors:
        vector_resource = forge.as_json([vector_resource])[0]
        point_id = get_id_attribute(vector_resource)
        vector = vector_resource["embedding"]
        boosting_factors[point_id] = 1 + compute_score_deviation(
            forge, point_id, vector, stats.min, stats.max,
            neighborhood_size, formula)

    return boosting_factors


def get_score_stats(forge, config: SimilaritySearchQueryConfiguration, boosted=False):
    """Retrieve view statistics."""
    view_id = config.statistics_view.id
    if view_id is None:
        raise SimilaritySearchException("Statistics view is not defined")

    ElasticSearch.set_elastic_view(forge, view_id)

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

    if len(statistics) == 0:
        raise SimilaritySearchException("No view statistics found")

    if len(statistics) > 1:
        print("Multiple statistics found, only getting the first one")
        pass

    statistics = forge.as_json(statistics)[0]
    statistics = dict((el["statistic"], el) for el in statistics["series"])

    return statistics["min"]["value"], statistics["max"]["value"]


def get_boosting_factors(forge, config: SimilaritySearchQueryConfiguration):
    """Retrieve boosting factors."""
    view_id = config.boosting_view.id

    if view_id is None:
        raise SimilaritySearchException("Boosting view is not defined")

    ElasticSearch.set_elastic_view(forge, view_id)

    factors = ElasticSearch.get_all_documents(forge)

    if len(factors) == 0:
        raise SimilaritySearchException("No boosting factors found")

    factors_json = forge.as_json(factors)

    boosting_factors = dict(
        (get_id_attribute(el["derivation"]["entity"]), el["value"])
        for el in factors_json
    )

    return boosting_factors


def combine_similarity_models(config: List[SimilaritySearchQueryConfiguration],
                              models_to_ignore: List[str],
                              forge_factory: Callable[[str, str], KnowledgeGraphForge],
                              query: SimilaritySearchQuery, parameter_values: Dict, k: int):

    # Perform similarity search combining several similarity models
    vector_ids = {}
    all_neighbors = {}
    stats = {}
    all_boosting_factors = {}
    all_boosted_stats = {}

    forge = [forge_factory(qc.org, qc.project) for qc in query.query_configurations]

    for i, individual_config in enumerate(config):
        # get the model ID and check if it's not in the list of models to ignore HERE
        model_id = individual_config.embedding_model.id

        if model_id is None:
            raise SimilaritySearchException(
                "Model is not defined, cannot retrieve similarity score formula")

        if model_id not in models_to_ignore:
            view_id = individual_config.similarity_view.id

            if view_id is None:
                raise SimilaritySearchException("Similarity search view is not defined")

            ElasticSearch.set_elastic_view(forge[i], view_id)

            vector_id, neighbors = query_similar_resources(
                forge_factory, forge[i], query, individual_config,
                parameter_values, k=None
            )

            vector_ids[i] = vector_id
            all_neighbors[i] = neighbors
            stats[i] = get_score_stats(forge[i], individual_config)

            if individual_config.boosted:
                all_boosting_factors[i] = get_boosting_factors(forge[i], individual_config)
                all_boosted_stats[i] = get_score_stats(forge[i], individual_config, boosted=True)

    # Combine the results
    combined_results = defaultdict(list)

    for i in all_neighbors.keys():
        neighbor_collection = all_neighbors[i]
        min_score, max_score = stats[i]
        boosting_factor = 1
        boosted_min, boosted_max = min_score, max_score

        if i in all_boosting_factors:
            # We need to boost the scores
            boosting_factor = all_boosting_factors[i][vector_ids[i]]
            boosted_min, boosted_max = all_boosted_stats[i]

        for score, n in neighbor_collection:
            resource_id = get_id_attribute(n["derivation"]["entity"])
            score = score * boosting_factor
            score = (score - boosted_min) / (boosted_max - boosted_min)
            combined_results[resource_id].append(score)

    combined_results = {
        key: np.array(value).mean()
        for key, value in combined_results.items()
    }

    neighbors = [
        {"id": el} for el in
        pd.DataFrame(
            combined_results.items(), columns=["result", "score"]
        ).nlargest(k, columns=["score"])["result"].tolist()
    ]

    return neighbors
