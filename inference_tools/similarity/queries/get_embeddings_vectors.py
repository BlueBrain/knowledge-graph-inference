import json

from typing import Dict, List

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.similarity.embedding import Embedding
from inference_tools.helper_functions import _enforce_list
from inference_tools.exceptions.exceptions import SimilaritySearchException
from inference_tools.similarity.queries.common import _find_derivation_id


def get_embedding_vectors(
        forge: KnowledgeGraphForge,
        search_targets: List[str],
        debug: bool,
        derivation_type: str,
        use_resources: bool,
        view: str = None
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
    use_resources : bool
    derivation_type: str in order to retrieve the derivation entity id, its type is needed to
    filter out the many entities in the derivation
    view : Optional[str]
        an elastic view to use, other than the one set in the forge instance, optional
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

    get_embedding_vectors_fc = _get_embedding_vectors if use_resources else \
        _get_embedding_vectors_json

    results: List[Dict] = get_embedding_vectors_fc(
        forge=forge, query=vector_query,
        debug=debug, search_targets=search_targets, view=view,
        derivation_type=derivation_type
    )

    return [Embedding(res) for res in results]


def _get_embedding_vectors(
        forge: KnowledgeGraphForge, query: Dict, debug: bool, search_targets: List[str],
        derivation_type: str,
        view: str = None
) -> List[Dict]:

    result = forge.elastic(json.dumps(query), limit=None, debug=debug, view=view)

    if result is None or len(result) == 0:
        raise SimilaritySearchException(f"No embedding vector for {search_targets}")

    return [
        {
            "id": e.id,
            "embedding": e.embedding,
            "derivation": _find_derivation_id(
                derivation_field=_enforce_list(forge.as_json(e)["derivation"]),
                type_=derivation_type
            )
        } for e in result
    ]


def _get_embedding_vectors_json(
        forge: KnowledgeGraphForge, query: Dict, debug: bool, search_targets: List[str],
        derivation_type: str,
        view: str = None
) -> List[Dict]:

    query["_source"] = ["embedding", "derivation.entity.@id", "derivation.entity.@type"]

    result = forge.elastic(json.dumps(query), limit=None, debug=debug, view=view, as_resource=False)

    if result is None or len(result) == 0:
        raise SimilaritySearchException(f"No embedding vector for {search_targets}")

    return [{
        "id": res["_id"],
        "embedding": res["_source"]["embedding"],
        "derivation": _find_derivation_id(
            derivation_field=_enforce_list(res["_source"]["derivation"]), type_=derivation_type
        )
    } for res in result]
