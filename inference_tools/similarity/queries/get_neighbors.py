# This file is part of knowledge-graph-inference.
# Copyright 2024 Blue Brain Project / EPFL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json

from string import Template
from typing import Optional, List, Dict, Tuple, Any

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.similarity.neighbor import Neighbor
from inference_tools.helper_functions import _enforce_list
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
        k: Optional[int] = DEFAULT_LIMIT,
        score_formula: Formula = Formula.EUCLIDEAN,
        result_filter=None,
        parameters=None,
        use_resources: bool = False,
        restricted_ids: Optional[List[str]] = None,
        specified_derivation_type=None,
        view: Optional[str] = None
) -> List[Tuple[int, Neighbor]]:
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
    use_resources: bool, optional
        Whether to retrieve the neighbors (embeddings) or not. May be used when performing
        similarity search, but not necessary when computing statistics
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

    similarity_query: Dict[str, Any] = {
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

    get_neighbors_fc = _get_neighbors if use_resources else _get_neighbors_json

    return get_neighbors_fc(
        forge, similarity_query, debug=debug,
        derivation_type=specified_derivation_type, view=view
    )


def _get_neighbors(
    forge: KnowledgeGraphForge, similarity_query: Dict, debug: bool,
    derivation_type: str, view: Optional[str] = None
) -> List:

    run = forge.elastic(json.dumps(similarity_query), limit=None, debug=debug, view=view)

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


def _get_neighbors_json(
    forge: KnowledgeGraphForge, similarity_query: Dict, debug: bool,
    derivation_type: str, view: Optional[str] = None
) -> List:

    similarity_query["_source"] = ["derivation.entity.@id", "derivation.entity.@type"]

    run = forge.elastic(
        json.dumps(similarity_query), limit=None, debug=debug, view=view, as_resource=False
    )

    if run is None or len(run) == 0:
        raise SimilaritySearchException("Getting neighbors failed")

    return [
        (e["_score"], Neighbor(
            _find_derivation_id(
                derivation_field=_enforce_list(e["_source"]["derivation"]), type_=derivation_type
            )
        ))
        for e in run
    ]
