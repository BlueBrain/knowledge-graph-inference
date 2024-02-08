from typing import Dict

import requests
import json

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.query_configuration import SimilaritySearchQueryConfiguration
from inference_tools.datatypes.similarity.boosting_factor import BoostingFactor

from inference_tools.exceptions.exceptions import SimilaritySearchException

from inference_tools.nexus_utils.delta_utils import DeltaUtils, DeltaException
from inference_tools.nexus_utils.forge_utils import ForgeUtils
from inference_tools.similarity.queries.common import _find_derivation_id


def get_boosting_factor_for_embedding(
        forge: KnowledgeGraphForge, embedding_id: str,
        config: SimilaritySearchQueryConfiguration,
        use_forge: bool
) -> BoostingFactor:
    """Retrieve boosting factors."""

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

    result: Dict = get_boosting_factors_fc(forge, query, config)

    return BoostingFactor(result)


def _get_boosting_factor_forge(
        forge: KnowledgeGraphForge, query: Dict, config: SimilaritySearchQueryConfiguration
) -> Dict:

    factor = forge.elastic(json.dumps(query), view=config.boosting_view.id)

    if factor is None or len(factor) == 0:
        raise SimilaritySearchException("No boosting factor found")

    return forge.as_json(factor)[0]


def _get_boosting_factor_delta(
        forge: KnowledgeGraphForge, query: Dict, config: SimilaritySearchQueryConfiguration
) -> Dict:

    url = forge._store.service.make_endpoint(
        view=config.boosting_view.id, endpoint_type="elastic"
    )
    token = ForgeUtils.get_token(forge)

    query["_source"] = ["derivation.entity.@id", "value"]

    run = DeltaUtils.check_response(
        requests.post(url=url, json=query, headers=DeltaUtils.make_header(token))
    )

    try:
        factor = DeltaUtils.check_hits(run)
    except DeltaException:
        raise SimilaritySearchException("No boosting factors found")

    if factor is None or len(factor) == 0:
        raise SimilaritySearchException("No boosting factor found")

    factor = factor[0]

    return {
        "value": factor["_source"]["value"],
        "derivation": {
            "entity": {
                "id": _find_derivation_id(factor["_source"]["derivation"], type_="Embedding")
            }
        }
    }
