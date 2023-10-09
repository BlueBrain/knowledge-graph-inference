from typing import Dict, List

import requests
import json

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.query_configuration import SimilaritySearchQueryConfiguration
from inference_tools.datatypes.similarity.boosting_factor import BoostingFactor

from inference_tools.exceptions.malformed_rule import MalformedSimilaritySearchQueryException
from inference_tools.exceptions.exceptions import SimilaritySearchException

from inference_tools.nexus_utils.delta_utils import DeltaUtils, DeltaException
from inference_tools.nexus_utils.forge_utils import ForgeUtils

from inference_tools.source.elastic_search import ElasticSearch


def get_boosting_factor_for_embedding(
        forge: KnowledgeGraphForge, embedding_id: str,
        config: SimilaritySearchQueryConfiguration,
        use_forge: bool
) -> BoostingFactor:
    """Retrieve boosting factors."""

    if config.boosting_view.id is None:
        raise MalformedSimilaritySearchQueryException("Boosting view is not defined")

    ForgeUtils.set_elastic_search_view(forge, config.boosting_view.id)

    get_boosting_factors_fc = _get_boosting_factor_forge if use_forge else \
        _get_boosting_factor_delta

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

    result: Dict = get_boosting_factors_fc(forge, query)

    return BoostingFactor(result)


def _get_boosting_factor_forge(forge: KnowledgeGraphForge, query: Dict) -> Dict:

    factor = forge.elastic(json.dumps(query))

    if factor is None or len(factor) == 0:
        raise SimilaritySearchException("No boosting factor found")

    return forge.as_json(factor)


def _get_boosting_factor_delta(forge: KnowledgeGraphForge, query: Dict) -> Dict:
    url = ForgeUtils.get_elastic_search_endpoint(forge)
    token = ForgeUtils.get_token(forge)

    query["_source"] = ["derivation.entity.@id", "value"]

    run = DeltaUtils.check_response(
        requests.post(url=url, json=query, headers=DeltaUtils.make_header(token))
    )

    try:
        factor = DeltaUtils.check_hits(run)
    except DeltaException:
        raise SimilaritySearchException("No boosting factors found")

    def change_el(el):
        return {
            "value": el["_source"]["value"],
            "derivation": {"entity": {"id": el["_source"]["derivation"]["entity"]["@id"]}}
        }

    if factor is None or len(factor) == 0:
        raise SimilaritySearchException("No boosting factor found")

    return change_el(factor[0])

