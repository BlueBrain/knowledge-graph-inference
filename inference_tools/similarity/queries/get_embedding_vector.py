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

# pylint: disable=R0801
import json

from typing import Optional, Dict

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.similarity.embedding import Embedding
from inference_tools.helper_functions import _enforce_list, get_id_attribute

from inference_tools.exceptions.exceptions import SimilaritySearchException
from inference_tools.similarity.queries.common import _find_derivation_id


def _err_message(entity_id: str, model_name: str) -> str:
    return f"{entity_id} was not embedded by the model {model_name}"


def get_embedding_vector(
        forge: KnowledgeGraphForge, search_target: str, debug: bool,
        model_name: str, use_resources: bool, derivation_type: str, view: Optional[str] = None
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
    use_resources : bool
    view : Optional[str]
        an elastic view to use, other than the one set in the forge instance, optional
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

    get_embedding_vector_fc = \
        _get_embedding_vector if use_resources else _get_embedding_vector_json

    result = get_embedding_vector_fc(
        forge=forge,
        query=vector_query, debug=debug,
        search_target=search_target, model_name=model_name,
        derivation_type=derivation_type,
        view=view
    )

    return Embedding(result)


def _get_embedding_vector(
        forge: KnowledgeGraphForge, query: Dict, debug: bool, search_target: str, model_name: str,
        derivation_type: str, view: Optional[str] = None
) -> Dict:

    result = forge.elastic(query=json.dumps(query), limit=None, debug=debug, view=view)

    if result is None or len(result) == 0:
        raise SimilaritySearchException(_err_message(search_target, model_name))

    e = forge.as_json(result[0])

    return {
        "id": get_id_attribute(e),
        "embedding": e["embedding"],
        "derivation": _find_derivation_id(
            derivation_field=_enforce_list(e["derivation"]), type_=derivation_type
        )
    }


def _get_embedding_vector_json(
        forge: KnowledgeGraphForge, query: Dict, debug: bool, search_target: str, model_name: str,
        derivation_type: str, view: Optional[str] = None
) -> Dict:

    query["_source"] = ["embedding", "derivation.entity.@id", "derivation.entity.@type"]

    result = forge.elastic(
        query=json.dumps(query), limit=None, debug=debug, view=view, as_resource=False
    )

    if result is None or len(result) == 0:
        raise SimilaritySearchException(_err_message(search_target, model_name))

    result = result[0]

    return {
        "id": result["_id"],
        "embedding": result["_source"]["embedding"],
        "derivation": _find_derivation_id(
            derivation_field=_enforce_list(result["_source"]["derivation"]), type_=derivation_type
        )
    }
