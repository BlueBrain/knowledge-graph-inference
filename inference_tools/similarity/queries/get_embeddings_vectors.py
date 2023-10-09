import json
import requests

from typing import Dict, List

from kgforge.core import KnowledgeGraphForge, Resource

from inference_tools.datatypes.similarity.embedding import Embedding
from inference_tools.helper_functions import _enforce_list
from inference_tools.nexus_utils.delta_utils import DeltaUtils, DeltaException
from inference_tools.nexus_utils.forge_utils import ForgeUtils
from inference_tools.exceptions.exceptions import SimilaritySearchException


def get_embedding_vectors(
        forge: KnowledgeGraphForge,
        search_targets: List[str],
        debug: bool,
        derivation_type: str,
        use_forge=False,
        es_endpoint: str = None
) -> List[Embedding]:
    """Get embedding vector for the target of the input similarity query.

    Parameters
    ----------
    forge : KnowledgeGraphForge
        Instance of a forge session
    search_targets : List[str]
        Value of the search target (usually, a resource ID for which we
        want to retrieve its nearest neighbors).
    debug : bool
    use_forge : bool
    derivation_type: str in order to retrieve the derivation entity id, its type is needed to
    filter out the many entities in the derivation
    es_endpoint : Optional[str]
        an elastic search endpoint to use, other than the one set in the forge instance, optional
    Returns
    -------
    """

    vector_query = {
        "from": 0,
        "size": len(search_targets),
        "query": {
            "bool": {
                "must": [
                    {
                        "nested": {
                            "path": "derivation.entity",
                            "query": {
                                "terms": {"derivation.entity.@id": search_targets}
                            }
                        }
                    },
                    {
                        "term": {
                            "_deprecated": False
                        }
                    }
                ]
            }
        }
    }

    get_embedding_vectors_fc = _get_embedding_vectors_forge if use_forge else \
        _get_embedding_vectors_delta

    results: List[Dict] = get_embedding_vectors_fc(
        forge=forge, query=vector_query,
        debug=debug, search_targets=search_targets, es_endpoint=es_endpoint,
        derivation_type=derivation_type
    )

    return [Embedding(res) for res in results]


def _get_embedding_vectors_forge(
        forge: KnowledgeGraphForge, query: Dict, debug: bool, search_targets: List[str],
        derivation_type: str,
        es_endpoint: str = None
) -> List[Dict]:

    tmp_endpoint = ForgeUtils.get_elastic_search_endpoint(forge)
    ForgeUtils.set_elastic_search_endpoint(forge, es_endpoint)
    result = forge.elastic(json.dumps(query), limit=None, debug=debug)
    ForgeUtils.set_elastic_search_endpoint(forge, tmp_endpoint)

    if result is None or len(result) == 0:
        raise SimilaritySearchException(f"No embedding vector for {search_targets}")

    def _find_derivation_id(obj: Resource, type_):
        dict_res = forge.as_json(obj)
        der = _enforce_list(dict_res["derivation"])
        el = next(e for e in der if e["entity"]["type"] == type_)
        return el["entity"]["id"]

    return [
        {
            "id": e.id,
            "embedding": e.embedding,
            "derivation": _find_derivation_id(e, type_=derivation_type)
        } for e in result
    ]


def _get_embedding_vectors_delta(
        forge: KnowledgeGraphForge, query: Dict, debug: bool, search_targets: List[str],
        derivation_type: str,
        es_endpoint: str = None
) -> List[Dict]:
    es_endpoint = es_endpoint if es_endpoint else ForgeUtils.get_elastic_search_endpoint(forge)

    token = ForgeUtils.get_token(forge)

    query["_source"] = ["embedding", "derivation.entity.@id", "derivation.entity.@type"]

    if debug:
        print(json.dumps(query, indent=4))

    result = DeltaUtils.check_response(
        requests.post(url=es_endpoint, json=query, headers=DeltaUtils.make_header(token))
    )

    try:
        result = DeltaUtils.check_hits(result)
    except DeltaException:
        raise SimilaritySearchException(f"No embedding vector for {search_targets}")

    if len(result) == 0:
        raise SimilaritySearchException(f"No embedding vector for {search_targets}")

    def _find_derivation_id(derivation_field, type_):
        der = _enforce_list(derivation_field)
        el = next(e for e in der if e["entity"]["@type"] == type_)
        return el["entity"]["@id"]

    return [{
        "id": res["_id"],
        "embedding": res["_source"]["embedding"],
        "derivation": _find_derivation_id(res["_source"]["derivation"], type_=derivation_type)
    } for res in result]
