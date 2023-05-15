import json
from typing import Dict, Optional, List
from urllib.parse import quote_plus
import requests

from kgforge.core import KnowledgeGraphForge, Resource

from inference_tools.bucket_configuration import BucketConfiguration
from inference_tools.datatypes.query import ElasticSearchQuery
from inference_tools.datatypes.query_configuration import ElasticSearchQueryConfiguration
from inference_tools.premise_execution import PremiseExecution
from inference_tools.source.source import Source, DEFAULT_LIMIT

DEFAULT_ES_VIEW = "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-es/dataset"
# TODO get rid of the edit of views


class ElasticSearch(Source):

    @staticmethod
    def execute_query(forge: KnowledgeGraphForge, query: ElasticSearchQuery,
                      parameter_values: Dict,
                      config: ElasticSearchQueryConfiguration, limit=DEFAULT_LIMIT,
                      debug: bool = False) -> Optional[List[Resource]]:

        if config.elastic_search_view is not None:
            ElasticSearch.set_elastic_view(forge, config.elastic_search_view.id)

        query_body = json.dumps(query.body)

        for k, v in parameter_values.items():
            query_body = query_body.replace(f"\"${k}\"", v)

        return forge.elastic(query_body, limit=limit, debug=debug)

    @staticmethod
    def check_premise(forge: KnowledgeGraphForge, premise: ElasticSearchQuery,
                      parameter_values: Dict,
                      config: ElasticSearchQueryConfiguration, debug: bool = False):

        results = ElasticSearch.execute_query(forge=forge, query=premise,
                                              parameter_values=parameter_values,
                                              debug=debug, config=config, limit=None)

        return PremiseExecution.SUCCESS if results is not None and len(results) > 0 else \
            PremiseExecution.FAIL

    @staticmethod
    def get_elastic_view_endpoint(forge: KnowledgeGraphForge):
        return ElasticSearch.get_store(forge).service.elastic_endpoint["endpoint"]

    @staticmethod
    def set_elastic_view_endpoint(forge: KnowledgeGraphForge, endpoint: str):
        ElasticSearch.get_store(forge).service.elastic_endpoint["endpoint"] = endpoint

    @staticmethod
    def set_elastic_view(forge: KnowledgeGraphForge, view: str):
        bucket_configuration = ElasticSearch.get_bucket_configuration(forge)

        views_endpoint = "/".join((
            bucket_configuration.endpoint,
            "views",
            quote_plus(bucket_configuration.organisation),
            quote_plus(bucket_configuration.project)
        ))
        endpoint = "/".join((views_endpoint, quote_plus(view), "_search"))

        ElasticSearch.set_elastic_view_endpoint(forge, endpoint)

    @staticmethod
    def get_all_documents(forge: KnowledgeGraphForge) -> Optional[List[Resource]]:
        """
        Retrieves all Resources that are indexed by the current elastic view endpoint of the forge
        instance
        @param forge: the forge instance
        @type forge: KnowledgeGraphForge
        @return:
        @rtype:  Optional[List[Resource]]
        """
        return forge.elastic(json.dumps(
            {
                "query": {
                    "term": {
                        "_deprecated": False
                    }
                }
            }), limit=10000)

    @staticmethod
    def get_by_ids(ids: List[str], forge: KnowledgeGraphForge) -> Optional[List[Resource]]:
        """

        @param ids: the list of ids of the resources to retrieve
        @type ids: List[str]
        @param forge: a forge instance
        @type forge: KnowledgeGraphForge
        @return: the list of Resources retrieved, if successful else None
        @rtype: Optional[List[Resource]]
        """
        q = {
            "size": 10000,
            'query': {
                'bool': {
                    'filter': [
                        {'terms': {'@id': ids}}
                    ],
                    'must': [
                        {'match': {'_deprecated': False}}
                    ]
                }
            },
        }

        return forge.elastic(json.dumps(q), debug=False)

    @staticmethod
    def check_view_readiness(bucket_config: BucketConfiguration, view_id: str, token: str) -> bool:
        """
        Make sure within a view's statistics, the last event datetime is equal to the last
        processed event's datetime
        @param bucket_config: the bucket in which the view is located
        @type bucket_config:
        @param view_id: the id of the view being checked
        @type view_id: str
        @param token: the authentication token
        @type token: str
        @return: Whether the view's last event datetime is equal to the last processed event's
        datetime
        @rtype: bool
        """
        view_id = quote_plus(view_id)
        url = f"{bucket_config.endpoint}/views/{bucket_config.organisation}/" \
              f"{bucket_config.project}/{view_id}/statistics"

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
