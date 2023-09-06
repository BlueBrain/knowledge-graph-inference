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


def get_boosting_factors(forge, config: SimilaritySearchQueryConfiguration, use_forge: bool) \
        -> Dict[str, BoostingFactor]:
    """Retrieve boosting factors."""

    if config.boosting_view.id is None:
        raise MalformedSimilaritySearchQueryException("Boosting view is not defined")

    ForgeUtils.set_elastic_search_view(forge, config.boosting_view.id)

    get_boosting_factors_fc = _get_boosting_factors_forge if use_forge else \
        _get_boosting_factors_delta

    query = ElasticSearch.get_all_documents_query()

    result: List[Dict] = get_boosting_factors_fc(forge, query)

    boosting_factors = [BoostingFactor(el) for el in result]

    return dict((el.entity_id, el) for el in boosting_factors)


def _get_boosting_factors_forge(forge: KnowledgeGraphForge, query: Dict) -> List[Dict]:

    factors = forge.elastic(json.dumps(query))

    if factors is None or len(factors) == 0:
        raise SimilaritySearchException("No boosting factors found")

    return forge.as_json(factors)


def _get_boosting_factors_delta(forge: KnowledgeGraphForge, query: Dict) -> List[Dict]:
    url = ForgeUtils.get_elastic_search_endpoint(forge)
    token = ForgeUtils.get_token(forge)

    query["_source"] = ["derivation.entity.@id", "value"]

    run = DeltaUtils.check_response(
        requests.post(url=url, json=query, headers=DeltaUtils.make_header(token))
    )

    try:
        run = DeltaUtils.check_hits(run)
    except DeltaException:
        raise SimilaritySearchException("No boosting factors found")

    def change_el(el):
        return {
            "value": el["_source"]["value"],
            "derivation": {"entity": {"id": el["_source"]["derivation"]["entity"]["@id"]}}
        }

    return list(map(change_el, run))

