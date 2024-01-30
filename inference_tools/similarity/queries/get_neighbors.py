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
from inference_tools.similarity.queries.common import _find_derivation_id
from inference_tools.source.source import DEFAULT_LIMIT


def get_neighbors(
        forge: KnowledgeGraphForge,
        vector: List[float],
        vector_id: str,
        debug: bool,
        derivation_type: str,
        k: int = DEFAULT_LIMIT,
        score_formula: Formula = Formula.EUCLIDEAN,
        result_filter=None,
        parameters=None,
        use_forge: bool = False,
        get_derivation: bool = False,
        restricted_ids: Optional[List[str]] = None,
        specified_derivation_type=None
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
    restricted_ids: List, optional
        A list of entity ids for which the associated embedding's score should be computed.
        Only these should be returned if specified. Else the top embedding scores will be returned
    debug: bool
    derivation_type: str : Used to find the appropriate derivation within the list of derivations
    in the embedding resource
    specified_derivation_type: str : Optional subtype of derivation_type, if only neighbors of
    this subtype should be returned

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
                        "must": [{
                            "exists": {"field": "embedding"}
                        }]
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

    if specified_derivation_type:  # If only a subtype of derivation_type can be a neighbor
        similarity_query["query"]["script_score"]["query"]["bool"]["must"].append(
            {
                "nested": {
                    "path": "derivation.entity",
                    "query": {
                        "term": {"derivation.entity.@type": specified_derivation_type}
                    }
                }
            }
        )
    else:
        specified_derivation_type = derivation_type

    if restricted_ids is not None:
        # Used to retrieve the distance between the provided embedding's source resource
        # and this specific set of resources
        similarity_query["query"]["script_score"]["query"]["bool"]["must"].append(
            {
                "nested": {
                    "path": "derivation.entity",
                    "query": {
                        "terms": {"derivation.entity.@id": restricted_ids}
                    }
                }
            }
        )

    if result_filter:
        if parameters:
            result_filter = Template(result_filter).substitute(parameters)

        similarity_query["query"]["script_score"]["query"]["bool"].update(json.loads(result_filter))

    get_neighbors_fc = _get_neighbors_forge if use_forge else _get_neighbors_delta

    return get_neighbors_fc(
        forge, similarity_query, debug=debug, get_derivation=get_derivation,
        derivation_type=specified_derivation_type
    )


def _get_neighbors_forge(
    forge: KnowledgeGraphForge, similarity_query: Dict, debug: bool, get_derivation: bool,
    derivation_type: str
) -> List:

    run = forge.elastic(json.dumps(similarity_query), limit=None, debug=debug)

    if run is None or len(run) == 0:
        raise SimilaritySearchException("Getting neighbors failed")
    return [
        (
            el._store_metadata._score,
            Neighbor(
                _find_derivation_id(
                    derivation_field=_enforce_list(forge.as_json(el)["derivation"]),
                    type_=derivation_type
                )
            )
        )
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

    return [
        (e["_score"], Neighbor(
            _find_derivation_id(
                derivation_field=_enforce_list(e["_source"]["derivation"]), type_=derivation_type
            )
        ) if get_derivation else None)
        for e in run
    ]


