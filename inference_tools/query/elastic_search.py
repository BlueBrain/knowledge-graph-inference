from urllib.parse import quote_plus
from string import Template
import requests
from inference_tools.query.source import Source
from inference_tools.type import PremiseType
from inference_tools.exceptions import UnsupportedTypeException
from inference_tools.helper_functions import _enforce_list, _safe_get_id_attribute

DEFAULT_ES_VIEW = "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-es/dataset"
# TODO get rid of the edit of views


class ElasticSearch(Source):

    @staticmethod
    def execute_query(forge, query, parameters, config=None, debug=False):

        custom_es_view = config.get("elasticSearchView", None) if config else None

        if custom_es_view is not None:
            ElasticSearch.set_elastic_view(forge, _safe_get_id_attribute(custom_es_view))

        query = Template(query["hasBody"]).substitute(**parameters)

        results = forge.as_json(forge.elastic(query, limit=10000, debug=debug))
        return results

    @staticmethod
    def check_premise(forge, premise, parameters, config, debug=False):
        raise UnsupportedTypeException(PremiseType.ELASTIC_SEARCH_PREMISE, "premise type")
        # TODO implement

    @staticmethod
    def get_elastic_view_endpoint(forge):
        return ElasticSearch.get_store(forge).service.elastic_endpoint["endpoint"]

    @staticmethod
    def set_elastic_view_endpoint(forge, endpoint):
        ElasticSearch.get_store(forge).service.elastic_endpoint["endpoint"] = endpoint

    @staticmethod
    def set_elastic_view(forge, view):
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
    def get_all_documents(forge):
        return forge.elastic("""
            {
              "query": {
                  "term": {
                      "_deprecated": false
                    }
                }
            }
        """, limit=10000)

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
    def restore_default_views(forge):
        forge = _enforce_list(forge)
        for f in forge:
            ElasticSearch.set_elastic_view(f, DEFAULT_ES_VIEW)
