"""Collection of utils for performing similarity search."""
import json

from string import Template
from typing import Callable, Optional


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

    vector_query = {
        "from": 0,
        "size": 1,
        "query": {
            "bool": {
                "must": [
                    {
                        "nested": {
                            "path": "derivation.entity",
                            "query": {
                                "terms": {"derivation.entity.@id": [search_target]}
                            }
                        }
                    }
                ]
            }
        }
    }

    result = forge.elastic(json.dumps(vector_query))

    if result is None or len(result) == 0:
        raise SimilaritySearchException(f"Could not get embedding vector for {search_target}")

    result = forge.as_json(result)[0]

    return get_id_attribute(result), result["embedding"]


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
        Must be parsable to a dict
        (e.g. "{'must': {'terms': {'tag': ['a', 'b', 'c']}} }" )).
    parameters : dict, optional
        Parameter dictionary to use in the provided `result_filter` statement.

    Returns
    -------
    result : list of tuples
        List of similarity search results, each element is a tuple with the
        score and the corresponding resource (json
        representation of the resource).
    """

    if k is None:
        k = 10000

    similarity_query = {
        "from": 0,
        "size": k,
        "query": {
            "script_score": {
                "query": {
                    "bool": {
                        "must_not": {
                            "term": {"@id": vector_id}
                        },
                        "must": {
                            "exists": {"field": "embedding"}
                        }
                    }
                },
                "script": {
                    "source": FORMULAS[score_formula],
                    "params": {
                        "query_vector": vector
                    }
                }
            }
        }
    }

    if result_filter:
        if parameters:
            result_filter = Template(result_filter).substitute(parameters)

        similarity_query["query"]["script_score"]["query"]["bool"].update(json.loads(result_filter))

    run = forge.elastic(json.dumps(similarity_query), limit=None)

    if run is None or len(run) == 0:
        print("utils/get_neighbors failed")

    result = [
        (el._store_metadata._score, forge.as_json(el))
        for el in run
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

    model_id = config.embedding_model.id

    if model_id is None:
        raise SimilaritySearchException(
            "Model is not defined, cannot retrieve similarity score formula")

    if config.embedding_model.org is not None and config.embedding_model.project is not None:
        model_forge = forge_factory(config.embedding_model.org, config.embedding_model.project)
    else:
        model_forge = forge

    # Get model revision, if specified
    if config.embedding_model.has_selector is not None:
        revision = config.embedding_model.has_selector.value
        model_id = model_id + revision

    model = model_forge.retrieve(model_id)
    score_formula = model.similarity

    # Search neighbors
    result = get_neighbors(
        forge, vector, vector_id, k, score_formula=score_formula,
        result_filter=query.result_filter, parameters=parameter_values)

    return vector_id, result


