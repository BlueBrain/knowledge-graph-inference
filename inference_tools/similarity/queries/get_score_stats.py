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
from inference_tools.datatypes.similarity.statistic import Statistic

from inference_tools.exceptions.exceptions import SimilaritySearchException


def get_score_stats(
        forge: KnowledgeGraphForge, config: SimilaritySearchQueryConfiguration,
        use_resources: bool, boosted: bool = False
) -> Statistic:
    """Retrieve view statistics."""

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"_deprecated": False}},
                    {"term": {"boosted": boosted}}
                ]
            }
        }
    }

    get_score_stats_fc = _get_score_stats if use_resources else _get_score_stats_json

    statistics = get_score_stats_fc(forge, query, config)

    return Statistic.from_json(statistics)


def _get_score_stats_json(
        forge: KnowledgeGraphForge, query: Dict, config: SimilaritySearchQueryConfiguration
) -> Dict:
    query["_source"] = ["series.*"]

    statistics = forge.elastic(json.dumps(query), view=config.statistics_view.id, as_resource=False)

    if statistics is None or len(statistics) == 0:
        raise SimilaritySearchException("No view statistics found")

    if len(statistics) > 1:
        print("Warning Multiple statistics found, only getting the first one")

    return statistics[0]["_source"]


def _get_score_stats(
        forge: KnowledgeGraphForge, query: Dict, config: SimilaritySearchQueryConfiguration
) -> Dict:
    statistics = forge.elastic(json.dumps(query), view=config.statistics_view.id)

    if statistics is None or len(statistics) == 0:
        raise SimilaritySearchException("No view statistics found")

    if len(statistics) > 1:
        print("Warning Multiple statistics found, only getting the first one")

    return forge.as_json(statistics[0])
