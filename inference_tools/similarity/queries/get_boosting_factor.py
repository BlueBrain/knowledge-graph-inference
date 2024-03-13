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

from typing import Dict

import json

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.query_configuration import SimilaritySearchQueryConfiguration
from inference_tools.datatypes.similarity.boosting_factor import BoostingFactor

from inference_tools.exceptions.exceptions import SimilaritySearchException
from inference_tools.helper_functions import _enforce_list

from inference_tools.similarity.queries.common import _find_derivation_id


def get_boosting_factor_for_embedding(
        forge: KnowledgeGraphForge, embedding_id: str,
        config: SimilaritySearchQueryConfiguration,
        use_resources: bool
) -> BoostingFactor:
    """Retrieve boosting factors."""

    get_boosting_factors_fc = _get_boosting_factor if use_resources else \
        _get_boosting_factor_json

    query = {
        "from": 0,
        "size": 1,
        "query": {
            "bool": {
                "must": [
                    {
                        "nested": {
                            "path": "derivation.entity",
                            "query": {
                                "term": {"derivation.entity.@id": embedding_id}
                            }
                        }
                    },
                    {
                        "term": {"_deprecated": False}
                    }
                ]
            }
        }
    }

    result: Dict = get_boosting_factors_fc(forge, query, config)

    return BoostingFactor(result)


def _get_boosting_factor(
        forge: KnowledgeGraphForge, query: Dict, config: SimilaritySearchQueryConfiguration
) -> Dict:

    factor = forge.elastic(json.dumps(query), view=config.boosting_view.id)

    if factor is None or len(factor) == 0:
        raise SimilaritySearchException("No boosting factor found")

    return forge.as_json(factor)[0]


def _get_boosting_factor_json(
        forge: KnowledgeGraphForge, query: Dict, config: SimilaritySearchQueryConfiguration
) -> Dict:

    query["_source"] = [
        "derivation.entity.@id",
        "derivation.entity.@type",
        "value"
    ]

    factor = forge.elastic(json.dumps(query), view=config.boosting_view.id, as_resource=False)

    if factor is None or len(factor) == 0:
        raise SimilaritySearchException("No boosting factor found")

    factor = factor[0]

    return {
        "value": factor["_source"]["value"],
        "derivation": {
            "entity": {
                "id": _find_derivation_id(
                    derivation_field=_enforce_list(factor["_source"]["derivation"]),
                    type_="Embedding"
                ),
                "type": "Embedding"
            }
        }
    }
