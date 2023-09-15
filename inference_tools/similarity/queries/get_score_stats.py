from typing import Dict

import requests
import json

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.query_configuration import SimilaritySearchQueryConfiguration
from inference_tools.datatypes.similarity.statistic import Statistic

from inference_tools.exceptions.malformed_rule import MalformedSimilaritySearchQueryException
from inference_tools.exceptions.exceptions import SimilaritySearchException

from inference_tools.nexus_utils.delta_utils import DeltaUtils, DeltaException
from inference_tools.nexus_utils.forge_utils import ForgeUtils


def get_score_stats(
        forge: KnowledgeGraphForge, config: SimilaritySearchQueryConfiguration,
        use_forge: bool, boosted: bool = False
) -> Statistic:
    """Retrieve view statistics."""

    if config.statistics_view.id is None:
        raise MalformedSimilaritySearchQueryException("Statistics view is not defined")

    ForgeUtils.set_elastic_search_view(forge, config.statistics_view.id)

    # TODO because now the targeted view is the aggregated,
    #  all model_views are being targeted
    #  instead of one...
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

    get_score_stats_fc = _get_score_stats_forge if use_forge else _get_score_stats_delta

    statistics = get_score_stats_fc(forge, query)

    return Statistic.from_json(statistics)


def _get_score_stats_delta(forge: KnowledgeGraphForge, query: Dict) -> Dict:
    url = ForgeUtils.get_elastic_search_endpoint(forge)
    token = ForgeUtils.get_token(forge)
    query["_source"] = ["series.*"]

    run = DeltaUtils.check_response(
        requests.post(url=url, json=query, headers=DeltaUtils.make_header(token))
    )

    try:
        run = DeltaUtils.check_hits(run)
    except DeltaException:
        raise SimilaritySearchException("No view statistics found")

    if len(run) > 1:
        print("Warning Multiple statistics found, only getting the first one")

    return run[0]["_source"]


def _get_score_stats_forge(forge: KnowledgeGraphForge, query: Dict) -> Dict:
    statistics = forge.elastic(json.dumps(query))

    if statistics is None or len(statistics) == 0:
        raise SimilaritySearchException("No view statistics found")

    if len(statistics) > 1:
        print("Warning Multiple statistics found, only getting the first one")

    return forge.as_json(statistics[0])
