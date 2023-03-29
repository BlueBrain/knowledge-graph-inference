import json
from typing import Dict
from urllib.parse import quote_plus
from string import Template
import requests

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.query import ElasticSearchQuery
from inference_tools.datatypes.query_configuration import ElasticSearchQueryConfiguration
from inference_tools.source.source import Source, DEFAULT_LIMIT
from inference_tools.type import PremiseType
from inference_tools.exceptions import UnsupportedTypeException
from inference_tools.helper_functions import _enforce_list

DEFAULT_ES_VIEW = "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-es/dataset"
# TODO get rid of the edit of views


class ElasticSearch(Source):

    @staticmethod
    def execute_query(forge: KnowledgeGraphForge, query: ElasticSearchQuery,
                      parameter_values: Dict,
                      config: ElasticSearchQueryConfiguration, limit=DEFAULT_LIMIT,
                      debug: bool = False):

        if config.elastic_search_view is not None:
            ElasticSearch.set_elastic_view(forge, config.elastic_search_view.id)

        query_body = Template(json.dumps(query.body)).substitute(**parameter_values)

        return forge.as_json(forge.elastic(query_body, limit=limit, debug=debug))

    @staticmethod
    def check_premise(forge: KnowledgeGraphForge, premise: ElasticSearchQuery,
                      parameter_values: Dict,
                      config: ElasticSearchQueryConfiguration, debug: bool = False):

        raise UnsupportedTypeException(PremiseType.ELASTIC_SEARCH_PREMISE, "premise type")
        # TODO implement

    @staticmethod
    def get_elastic_view_endpoint(forge: KnowledgeGraphForge):
        return ElasticSearch.get_store(forge).service.elastic_endpoint["endpoint"]

    @staticmethod
    def set_elastic_view_endpoint(forge: KnowledgeGraphForge, endpoint: str):
        ElasticSearch.get_store(forge).service.elastic_endpoint["endpoint"] = endpoint

    @staticmethod
    def set_elastic_view(forge: KnowledgeGraphForge, view: str):
        org, project = ElasticSearch.get_store(forge).bucket.split("/")[-2:]

        views_endpoint = "/".join((
            ElasticSearch.get_store(forge).endpoint,
            "views",
            quote_plus(org),
            quote_plus(project)
        ))
        endpoint = "/".join((views_endpoint, quote_plus(view), "_search"))

        ElasticSearch.set_elastic_view_endpoint(forge, endpoint)

    @staticmethod
    def get_all_documents(forge: KnowledgeGraphForge):
        return forge.elastic(json.dumps(
            {
              "query": {
                  "term": {
                      "_deprecated": False
                    }
                }
            }), limit=10000)

    @staticmethod
    def check_view_readiness(bucket_config, view_id, token):
        view_id = quote_plus(view_id)
        url = f"{bucket_config.endpoint}/views/{bucket_config.org}/" \
              f"{bucket_config.proj}/{view_id}/statistics"

        r = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"}
        )
        response = r.json()
        last_event = response["lastEventDateTime"]
        last_processed_event = None
        if "lastProcessedEventDateTime" in response:
            last_processed_event = response["lastProcessedEventDateTime"]
        return last_event == last_processed_event

    @staticmethod
    def restore_default_views(forge: KnowledgeGraphForge):
        ElasticSearch.set_elastic_view(forge, DEFAULT_ES_VIEW)
