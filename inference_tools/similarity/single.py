"""Collection of utils for performing similarity search."""
import json
import requests

from string import Template
from typing import Callable, Optional, List, Dict, Tuple

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.query_configuration import SimilaritySearchQueryConfiguration
from inference_tools.datatypes.similarity.embedding import Embedding
from inference_tools.datatypes.similarity.neighbor import Neighbor
from inference_tools.exceptions.malformed_rule import MalformedSimilaritySearchQueryException
from inference_tools.nexus_utils.delta_utils import DeltaUtils
from inference_tools.nexus_utils.forge_utils import ForgeUtils
from inference_tools.similarity.formula import Formula
from inference_tools.exceptions.exceptions import SimilaritySearchException


def get_embedding_vector(forge: KnowledgeGraphForge, search_target: str) -> Embedding:
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
        raise SimilaritySearchException(f"No embedding vector for {search_target}")

    result = forge.as_json(result)[0]

    return Embedding(result)


def get_neighbors(forge: KnowledgeGraphForge, vector: List[float], vector_id: str, debug: bool,
                  k: int = None, score_formula: Formula = Formula.EUCLIDEAN,
                  result_filter=None, parameters=None, get_payload: bool = True) \
        -> List[Tuple[int, Optional[Neighbor]]]:
    """Get nearest neighbors of the provided vector.

    Parameters
    ----------
    forge : KnowledgeGraphForge
        Instance of a forge session
    k: int
    vector : list
        Vector to provide into similarity search
    vector_id : str
        Id of the embedding resource corresponding to the
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
    get_payload: bool, optional
        Whether to retrieve the neighbors (embeddings) or not. Necessary when performing similarity
        search, but not when computing statistics
    parameters : dict, optional
        Parameter dictionary to use in the provided `result_filter` statement.
    debug: bool

    Returns
    -------
    result : list of tuples
        List of similarity search results, each element is a tuple with the
        score and the corresponding resource (json representation of the resource).
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
                    "source": score_formula.get_formula(),
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

    if get_payload:
        run = forge.elastic(json.dumps(similarity_query), limit=None, debug=debug)

        if run is None or len(run) == 0:
            raise SimilaritySearchException("Getting neighbors failed")

        return [
            (el._store_metadata._score, Neighbor(forge.as_json(el)))
            for el in run
        ]
    else:
        url = ForgeUtils.get_elastic_search_endpoint(forge)
        token = ForgeUtils.get_token(forge)
        similarity_query["_source"] = False

        run = DeltaUtils.check_response(
            requests.post(url=url, json=similarity_query, headers=DeltaUtils.make_header(token))
        )

        if "hits" not in run or len(run["hits"]) == 0:
            raise SimilaritySearchException("Getting neighbors failed")

        return [
            (e["_score"], None)
            for e in run["hits"]["hits"]
        ]


def query_similar_resources(forge_factory: Callable[[str, str], KnowledgeGraphForge],
                            forge: KnowledgeGraphForge,
                            config: SimilaritySearchQueryConfiguration,
                            parameter_values, k: Optional[int], target_parameter: str,
                            result_filter: Optional[str], debug: bool) \
        -> Tuple[str, List[Tuple[int, Neighbor]]]:
    """Query similar resources using the similarity query.

    Parameters
    ----------
    forge_factory : func
        Factory that returns a forge session given a bucket
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

    Returns
    -------
    result :  Tuple[str, Dict[int, Neighbor]]
        The id of the embedding vector of the resource being queried, as well as a dictionary
        with keys being scores and values being a Neighbor object holding
        the resource id that is similar

    """
    # Set ES view from the config
    if config.similarity_view.id is None:
        raise MalformedSimilaritySearchQueryException("Similarity search view is not defined")

    ForgeUtils.set_elastic_search_view(forge, config.similarity_view.id)

    search_target = parameter_values.get(target_parameter, None)  # TODO should it be formatted ?

    if search_target is None:
        raise SimilaritySearchException(f"Target parameter value is not specified, a value for the"
                                        f"parameter {target_parameter} is necessary")

    embedding = get_embedding_vector(forge, search_target)

    model_id = config.embedding_model.id

    if model_id is None:
        raise MalformedSimilaritySearchQueryException(
            "Model is not defined, cannot retrieve similarity score formula"
        )

    if config.embedding_model.org is not None and config.embedding_model.project is not None:
        model_forge = forge_factory(config.embedding_model.org, config.embedding_model.project)
    else:
        model_forge = forge

    # Get model revision, if specified
    if config.embedding_model.has_selector is not None:
        revision = config.embedding_model.has_selector.value
        model_id = model_id + revision

    model = model_forge.retrieve(model_id)
    score_formula = Formula(model.similarity)

    # Search neighbors
    result: List[Tuple[int, Neighbor]] = get_neighbors(
        forge=forge, vector=embedding.vector, vector_id=embedding.id,
        k=k, score_formula=score_formula,
        result_filter=result_filter, parameters=parameter_values, debug=debug
    )

    return embedding.id, result
