import json
import requests

from typing import Dict

from kgforge.core import KnowledgeGraphForge, Resource

from inference_tools.datatypes.similarity.embedding import Embedding
from inference_tools.helper_functions import _enforce_list, get_id_attribute
from inference_tools.nexus_utils.delta_utils import DeltaUtils, DeltaException
from inference_tools.nexus_utils.forge_utils import ForgeUtils
from inference_tools.exceptions.exceptions import SimilaritySearchException


def get_embedding_vector(
        forge: KnowledgeGraphForge, search_target: str, debug: bool,
        derivation_type: str, es_endpoint: str = None, use_forge=False
) -> Embedding:
    """Get embedding vector for the target of the input similarity query.

    Parameters
    ----------
    forge : KnowledgeGraphForge
        Instance of a forge session
    search_target : str
        Value of the search target (usually, a resource ID for which we
        want to retrieve its nearest neighbors).
    debug : bool
    use_forge : bool
    es_endpoint : Optional[str]
        an elastic search endpoint to use, other than the one set in the forge instance, optional
    Returns
    -------
    embedding : Embedding
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
                                "term": {"derivation.entity.@id": search_target}
                            }
                        }
                    }
                ]
            }
        }
    }

    get_embedding_vector_fc = \
        _get_embedding_vector_forge if use_forge else \
            _get_embedding_vector_delta

    result = get_embedding_vector_fc(
        forge, vector_query, debug, search_target, derivation_type, es_endpoint
    )

    return Embedding(result)


def _get_embedding_vector_forge(
        forge: KnowledgeGraphForge, query: Dict, debug: bool, search_target: str,
        derivation_type: str, es_endpoint: str = None
) -> Dict:

    result = forge.elastic(json.dumps(query), limit=None, debug=debug)

    if result is None or len(result) == 0:
        raise SimilaritySearchException(f"No embedding vector for {search_target}")

    def _find_derivation_id(obj: Dict, type_):
        der = _enforce_list(obj["derivation"])
        el = next(e for e in der if e["entity"]["@type"] == type_)
        return el["entity"]["@id"]

    e = forge.as_json(result[0])

    return {
        "id": get_id_attribute(e),
        "embedding": e["embedding"],
        "derivation": _find_derivation_id(e, type_=derivation_type)
    }


def _get_embedding_vector_delta(
        forge: KnowledgeGraphForge, query: Dict, debug: bool, search_target: str,
        derivation_type: str, es_endpoint: str = None
) -> Dict:

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
        raise SimilaritySearchException(f"No embedding vector for {search_target}")

    if len(result) == 0:
        raise SimilaritySearchException(f"No embedding vector for {search_target}")

    result = result[0]

    def _find_derivation_id(derivation_field, type_):
        der = _enforce_list(derivation_field)
        el = next(e for e in der if e["entity"]["@type"] == type_)
        return el["entity"]["@id"]

    return {
        "id": result["_id"],
        "embedding": result["_source"]["embedding"],
        "derivation": _find_derivation_id(result["_source"]["derivation"], type_=derivation_type)
    }
