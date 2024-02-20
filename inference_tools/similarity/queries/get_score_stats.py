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
