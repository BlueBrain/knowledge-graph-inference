"""Collection of utils for performing similarity search."""
import json
from string import Template
from collections import defaultdict, namedtuple

import math
from typing import Dict, Callable, List, Optional

import numpy as np
import pandas as pd
from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.query import SimilaritySearchQuery
from inference_tools.datatypes.query_configuration import SimilaritySearchQueryConfiguration
from inference_tools.helper_functions import get_id_attribute
from inference_tools.source.elastic_search import ElasticSearch
from inference_tools.exceptions import SimilaritySearchException

FORMULAS = {
    "cosine": "doc['embedding'].size() == 0 ? 0 : (cosineSimilarity(params.query_vector, doc['embedding']) + 1.0) / 2",
    "euclidean": "doc['embedding'].size() == 0 ? 0 : (1 / (1 + l2norm(params.query_vector, doc['embedding'])))",
    "poincare": "float[] v = doc['embedding'].vectorValue; if (doc['embedding'].size() == 0) { return 0; } double am = doc['embedding'].magnitude; double bm = 0; double dist = 0; for (int i = 0; i < v.length; i++) { bm += Math.pow(params.query_vector[i], 2); dist += Math.pow(v[i] - params.query_vector[i], 2); } bm = Math.sqrt(bm); dist = Math.sqrt(dist); double x = 1 + (2 * Math.pow(dist, 2)) / ( (1 - Math.pow(bm, 2)) * (1 - Math.pow(am, 2)) );  double d = Math.log(x + Math.sqrt(Math.pow(x, 2) - 1)); return 1 / (1 + d);"
}


def get_embedding_vector(forge: KnowledgeGraphForge, search_target):
    """Get embedding vector for the target of the input similarity query.

    Parameters
    ----------
    forge : KnowledgeGraphForge
        Instance of a forge session
    search_target : str
        Value of the search target (usually, a resource ID for which we
        want to retrieve its nearest neighbors).

    Returns
    -------
    vector_id : str
        ID of the resource corresponding to the search target
    vector : list
        Corresponding embedding vector
    """
    vector_query = """
        {
          "from": 0,
          "size": 1,
          "query": {
            "bool": {
              "must": [
                {
                  "nested": {
                    "path": "derivation.entity",
                    "query": {
                      "terms": {
                        "derivation.entity.@id": [$_searchTarget]
                      }
                    }
                  }
                }
              ]
            }
          }
        }
    """
    vector_query = Template(vector_query).substitute({"_searchTarget": search_target})

    result = forge.elastic(vector_query)

    if result is None or len(result) == 0:
        raise SimilaritySearchException(f"Could not get embedding vector for {search_target}")

    result = forge.as_json(result)[0]
    vector_id = get_id_attribute(result)
    vector = result["embedding"]
    return vector_id, vector


def get_neighbors(forge: KnowledgeGraphForge, vector, vector_id,
                  k=None, score_formula="euclidean",
                  result_filter=None, parameters=None):
    """Get nearest neighbors of the provided vector.

    Parameters
    ----------
    forge : KnowledgeGraphForge
        Instance of a forge session
    k: int
    vector : list
        Vector to provide into similarity search
    vector_id : str
        Id of the embedding resource  corresponding to the
        provided search vector (will be excluded in the
        similarity search).
    score_formula : str, optional
        Name of the formula to use for computing similarity scores,
        possible values: "euclidean" (default), "cosine", "poincare".
    result_filter : str, optional
        String representing a parametrized ES filter expression to append
        to the search query
        (e.g. "'must': {'terms': {'tag': ['a', 'b', 'c']}}")).
    parameters : dict, optional
        Parameter dictionary to use in the provided `result_filter` statement.

    Returns
    -------
    result : list of tuples
        List of similarity search results, each element is a tuple with the
        score and the corresponding resource (json
        representation of the resource).
    """
    # Preprocess result filter
    if result_filter:
        if parameters:
            result_filter = Template(result_filter).substitute(parameters)
        result_filter = ",\n" + result_filter
    else:
        result_filter = ""

    if k is None:
        k = 10000

    similarity_query = """
        {
          "from": 0,
          "size": $_k,
          "query": {
            "script_score": {
                "query": {
                    "bool" : {
                      "must_not" : {
                        "term" : { "@id": "$_vectorId" }
                      },
                      "must": { "exists": { "field": "embedding" } }
                      $_resultFilter
                    }
                },
                "script": {
                    "query": "$_formula",
                    "params": {
                      "query_vector": $_vector
                    }
                }
            }
          }
    }
    """

    similarity_query = Template(similarity_query).substitute({
        "_vectorId": vector_id,
        "_vector": vector,
        "_k": k,
        "_formula": FORMULAS[score_formula],
        "_resultFilter": result_filter
    })
    result = [
        (el._store_metadata._score, forge.as_json([el])[0])
        for el in forge.elastic(similarity_query, limit=None)
    ]
    return result


def query_similar_resources(forge_factory: Callable[[str, str], KnowledgeGraphForge],
                            forge: KnowledgeGraphForge,
                            query: SimilaritySearchQuery,
                            config: SimilaritySearchQueryConfiguration,
                            parameter_values, k: Optional[int]):
    """Query similar resources using the similarity query.

    Parameters
    ----------
    forge_factory : func
        Factory that returns a forge session given a bucket
    forge : KnowledgeGraphForge
        Instance of a forge session
    query : dict
        Json representation of the similarity search query (`SimilarityQuery`)
    config: dict or list of dict
        Query configuration containing references to the target views
        to be queried.
    parameter_values : dict
        Input parameters used in the similarity query
    k : int
        Number of nearest neighbors to query

    Returns
    -------
    result : list of tuples
        List of similarity search results, each element is a tuple with the
        score and the corresponding resource (json representation of
        the resource).
    """
    # Set ES view from the config
    view_id = config.similarity_view.id
    if view_id is None:
        raise SimilaritySearchException("Similarity search view is not defined")
    ElasticSearch.set_elastic_view(forge, view_id)

    # Get search target vector
    target_parameter = query.search_target_parameter
    if target_parameter is None:
        raise SimilaritySearchException("Target parameter is not specified")

    search_target = parameter_values.get(target_parameter, None)
    if search_target is None:
        raise SimilaritySearchException("Target parameter value is not specified")

    vector_id, vector = get_embedding_vector(forge, search_target)

    # TODO: Retrieve score formula from the model
    model_id = config.embedding_model.id

    if model_id is None:
        raise SimilaritySearchException(
            "Model is not defined, cannot retrieve similarity score formula")

    if config.embedding_model.org is not None and \
            config.embedding_model.project is not None:
        model_forge = forge_factory(config.embedding_model.org, config.embedding_model.project)
    else:
        model_forge = forge

    # Get model revision, if specified
    if config.embedding_model.has_selector is not None:
        revision = config.embedding_model.has_selector["value"]
        model_id = model_id + revision

    model = model_forge.retrieve(model_id)
    score_formula = model.similarity

    # Search neighbors
    result = get_neighbors(
        forge, vector, vector_id, k, score_formula=score_formula,
        result_filter=query.result_filter, parameters=parameter_values)

    return vector_id, result


def get_score_stats(forge, config: SimilaritySearchQueryConfiguration, boosted=False):
    """Retrieve view statistics."""
    view_id = config.statistics_view.id
    if view_id is None:
        raise SimilaritySearchException("Statistics view is not defined")
    ElasticSearch.set_elastic_view(forge, view_id)
    statistics = forge.elastic(json.dumps(
        {
            "query": {
                "bool": {
                    "must": {
                        "term": {
                            "_deprecated": False,
                            "boosted": boosted
                        }
                    }
                }
            }
        }))

    if len(statistics) == 0:
        raise SimilaritySearchException("No view statistics found")

    if len(statistics) > 1:
        # Here warn that more than one is found, we will use one of them
        pass
    statistics = forge.as_json(statistics)[0]
    min_score = None
    max_score = None
    for el in statistics["series"]:
        if el["statistic"] == "min":
            min_score = el["value"]
        if el["statistic"] == "max":
            max_score = el["value"]

    return min_score, max_score


def get_boosting_factors(forge, config: SimilaritySearchQueryConfiguration):
    """Retrieve boosting factors."""
    view_id = config.boosting_view.id

    if view_id is None:
        raise SimilaritySearchException("Boosting view is not defined")

    ElasticSearch.set_elastic_view(forge, view_id)

    factors = ElasticSearch.get_all_documents(forge)

    if len(factors) == 0:
        raise SimilaritySearchException("No boosting factors found")

    boosting_factors = {}
    for el in factors:
        json_el = forge.as_json([el])[0]
        boosting_factors[get_id_attribute(json_el["derivation"]["entity"])] = json_el["value"]

    return boosting_factors


def execute_similarity_query(forge_factory: Callable[[str, str], KnowledgeGraphForge],
                             forge: List[KnowledgeGraphForge],
                             query: SimilaritySearchQuery, parameter_values: Dict):
    """Execute similarity search query.

    Parameters
    ----------
    forge_factory : func
        Factory that returns a forge session given a bucket
    forge : List[KnowledgeGraphForge]
        Instance of a forge session
    query : dict
        Json representation of the similarity search query (`SimilarityQuery`)
    parameter_values : dict
        Input parameters used in the similarity query

    Returns
    -------
    neighbors : list of resource ID
        List of similarity search results, each element is a resource ID.
    """
    config: List[SimilaritySearchQueryConfiguration] = query.query_configurations

    if config is None:
        raise SimilaritySearchException("No similarity search configuration provided")

    k = query.k

    if isinstance(k, str):
        k = int(Template(k).substitute(parameter_values))

    models_to_ignore = parameter_values.get("IgnoreModelsParameter", [])

    if len(config) == 1:
        # Perform similarity search using a single similarity model
        _, neighbors = query_similar_resources(
            forge_factory, forge[0], query, config[0], parameter_values, k)

        neighbors = [
            {"id": get_id_attribute(n["derivation"]["entity"])}
            for _, n in neighbors
        ]
    else:
        # Perform similarity search combining several similarity models
        vector_ids = {}
        all_neighbors = {}
        stats = {}
        all_boosting_factors = {}
        all_boosted_stats = {}
        for i, individual_config in enumerate(config):
            # get the model ID and check if it's not in the
            # list of models to ignore HERE
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
                    all_boosted_stats[i] = get_score_stats(
                        forge[i], individual_config, boosted=True)

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
            {"id": el}
            for el in pd.DataFrame(
                combined_results.items(), columns=["result", "score"]).nlargest(
                k, columns=["score"])["result"].tolist()
        ]

    return neighbors


def compute_statistics(forge, view_id, score_formula, boosting=None):
    """Compute similarity score statistics given a view."""
    ElasticSearch.set_elastic_view(forge, view_id)
    all_vectors = ElasticSearch.get_all_documents(forge)

    scores = []
    for vector_resource in all_vectors:
        vector_resource = forge.as_json([vector_resource])[0]
        vector = vector_resource["embedding"]
        vector_id = get_id_attribute(vector_resource)
        neighbors = get_neighbors(
            forge, vector, vector_id,
            k=len(all_vectors), score_formula=score_formula)

        for score, el in neighbors:
            boost_factor = 1
            if boosting:
                boost_factor = boosting[vector_id]
            scores.append(score * boost_factor)

    scores = np.array(list(scores))
    Statistics = namedtuple('Statistics', 'min max mean std')
    return len(scores), Statistics(
        scores.min(), scores.max(),
        scores.mean(), scores.std())


def compute_score_deviation(forge, point_id, vector, score_min, score_max, k, formula):
    """Compute similarity score deviation for each vector."""
    query = f"""{{
      "size": {k},
      "query": {{
          "script_score": {{
              "query": {{
                    "exists": {{
                        "field": "embedding"
                }}
          }},
          "script": {{
              "query": "{FORMULAS[formula]}",
              "params": {{
                  "query_vector": {vector}
              }}
          }}
        }}
      }}
    }}"""

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
