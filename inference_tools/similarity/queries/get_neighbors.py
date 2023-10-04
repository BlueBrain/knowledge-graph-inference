import json
import requests

from string import Template
from typing import Optional, List, Dict, Tuple

from kgforge.core import KnowledgeGraphForge, Resource

from inference_tools.datatypes.similarity.neighbor import Neighbor
from inference_tools.helper_functions import _enforce_list
from inference_tools.nexus_utils.delta_utils import DeltaUtils, DeltaException
from inference_tools.nexus_utils.forge_utils import ForgeUtils
from inference_tools.similarity.formula import Formula
from inference_tools.exceptions.exceptions import SimilaritySearchException
from inference_tools.source.source import DEFAULT_LIMIT


def get_neighbors(
        forge: KnowledgeGraphForge,
        vector: List[float],
        vector_id: str,
        debug: bool,
        derivation_type: str,
        k: int = DEFAULT_LIMIT,
        score_formula: Formula = Formula.EUCLIDEAN,
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
    derivation_type: str

    Returns
    -------
    result : list of tuples
        List of similarity search results, each element is a tuple with the
        score and the corresponding resource (json representation of the resource).
    """

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

    return get_neighbors_fc(forge, similarity_query, debug=debug, get_derivation=get_derivation,
                            derivation_type=derivation_type)


def _get_neighbors_forge(
    forge: KnowledgeGraphForge, similarity_query: Dict, debug: bool, get_derivation: bool,
    derivation_type: str
) -> List:

    run = forge.elastic(json.dumps(similarity_query), limit=None, debug=debug)

    def _find_derivation_id(obj: Resource, type_):
        dict_res = forge.as_json(obj)
        der = _enforce_list(dict_res["derivation"])
        el = next(e for e in der if e["entity"]["@type"] == type_)
        return el["entity"]["@id"]

    if run is None or len(run) == 0:
        raise SimilaritySearchException("Getting neighbors failed")
    return [
        (el._store_metadata._score, Neighbor(
            _find_derivation_id(el, type_=derivation_type)
        ))
        for el in run
    ]


def _get_neighbors_delta(
    forge: KnowledgeGraphForge, similarity_query: Dict, debug: bool, get_derivation: bool,
    derivation_type: str
) -> List:

    url = ForgeUtils.get_elastic_search_endpoint(forge)
    token = ForgeUtils.get_token(forge)

    similarity_query["_source"] = ["derivation.entity.@id", "derivation.entity.@type"] if \
        get_derivation else \
        False

    if debug:
        print(similarity_query)

    run = DeltaUtils.check_response(
        requests.post(url=url, json=similarity_query, headers=DeltaUtils.make_header(token))
    )

    try:
        run = DeltaUtils.check_hits(run)
    except DeltaException:
        raise SimilaritySearchException("Getting neighbors failed")

    def _find_derivation_id(derivation_field, type_):
        der = _enforce_list(derivation_field)
        el = next(e for e in der if e["entity"]["@type"] == type_)
        return el["entity"]["@id"]

    return [
        (e["_score"], Neighbor(
            _find_derivation_id(e["_source"]["derivation"], type_=derivation_type)
        ) if get_derivation else None)
        for e in run
    ]


