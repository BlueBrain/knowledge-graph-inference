import json
import requests

from string import Template
from typing import Optional, List, Dict, Tuple

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.similarity.neighbor import Neighbor
from inference_tools.nexus_utils.delta_utils import DeltaUtils, DeltaException
from inference_tools.nexus_utils.forge_utils import ForgeUtils
from inference_tools.similarity.formula import Formula
from inference_tools.exceptions.exceptions import SimilaritySearchException


def get_neighbors(
        forge: KnowledgeGraphForge, vector: List[float], vector_id: str, debug: bool,
        k: int = None, score_formula: Formula = Formula.EUCLIDEAN,
        result_filter=None, parameters=None,
        use_forge: bool = False,
        get_derivation: bool = False
) -> List[Tuple[int, Optional[Neighbor]]]:
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
    use_forge: bool, optional
        Whether to retrieve the neighbors (embeddings) or not. May be used when performing
        similarity search, but not necessary when computing statistics
    get_derivation:
        Whether to retrieve the derivation (original entity) or not. Necessary when
        performing similarity search
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

    get_neighbors_fc = _get_neighbors_forge if use_forge else _get_neighbors_delta

    return get_neighbors_fc(forge, similarity_query, debug=debug, get_derivation=get_derivation)


def _get_neighbors_forge(
    forge: KnowledgeGraphForge, similarity_query: Dict, debug: bool, get_derivation: bool
) -> List:

    run = forge.elastic(json.dumps(similarity_query), limit=None, debug=debug)
    if run is None or len(run) == 0:
        raise SimilaritySearchException("Getting neighbors failed")
    return [
        (el._store_metadata._score, Neighbor(forge.as_json(el)))
        for el in run
    ]


def _get_neighbors_delta(
    forge: KnowledgeGraphForge, similarity_query: Dict, debug: bool, get_derivation: bool
) -> List:

    url = ForgeUtils.get_elastic_search_endpoint(forge)
    token = ForgeUtils.get_token(forge)

    similarity_query["_source"] = ["derivation.entity.@id"] if get_derivation else False

    run = DeltaUtils.check_response(
        requests.post(url=url, json=similarity_query, headers=DeltaUtils.make_header(token))
    )

    try:
        run = DeltaUtils.check_hits(run)
    except DeltaException:
        raise SimilaritySearchException("Getting neighbors failed")

    def reformat(element):
        id_ = element["_source"]["derivation"]["entity"]["@id"]
        return {"derivation": {"entity": {"@id": id_}}}

    return [
        (e["_score"], Neighbor(reformat(e)) if get_derivation else None)
        for e in run
    ]
