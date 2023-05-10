from typing import Dict, List, Optional
from urllib.parse import quote_plus as url_encode, quote_plus
import requests
import json

from inference_tools.bucket_configuration import NexusBucketConfiguration


class DeltaException(Exception):
    body: Dict
    status_code: int

    def __init__(self, body: Dict, status_code: int):
        self.body = body
        self.status_code = status_code


def make_header(token):
    return {
        "mode": "cors",
        "Content-Type": "application/json",
        "Accept": "application/ld+json, application/json",
        "Authorization": "Bearer " + token
    }


def check_response(response: requests.Response) -> Dict:
    if response.status_code not in range(200, 229):
        raise DeltaException(body=json.loads(response.text), status_code=response.status_code)
    return json.loads(response.text)


def create_es_view(
        bucket_configuration: NexusBucketConfiguration,
        token: str,
        es_view_id: str,
        pipeline: List[Dict],
        mapping: Optional[Dict] = None,
        resource_tag: str = None
) -> Dict:
    payload = {
        "@id": es_view_id,
        "@type": ["View", "ElasticSearchView"],
        "pipeline": pipeline,
        "mapping": mapping if mapping is not None else {}
    }

    if resource_tag is not None:
        payload["resourceTag"] = resource_tag

    url = f"{bucket_configuration.endpoint}/views/{url_encode(bucket_configuration.organisation)}" \
          f"/{url_encode(bucket_configuration.project)}"

    header = {
        "mode": "cors",
        "Content-Type": "application/json",
        "Accept": "application/ld+json, application/json",
        "Authorization": "Bearer " + token
    }

    return check_response(requests.post(url=url, headers=header, json=payload))


def get_es_view(
        bucket_configuration: NexusBucketConfiguration,
        token: str,
        es_view_id: str,
        with_metadata: bool = True
) -> Dict:
    url = f"{bucket_configuration.endpoint}/views/{url_encode(bucket_configuration.organisation)}" \
          f"/{url_encode(bucket_configuration.project)}/{quote_plus(es_view_id)}" \
          f"{'/source' if not with_metadata else ''}"

    return check_response(requests.get(url=url, headers=make_header(token)))


def update_es_view_resource_tag(
        bucket_configuration: NexusBucketConfiguration,
        token: str,
        es_view_id: str,
        view_body: Dict,
        resource_tag: str,
        rev: int
) -> Dict:
    url = f"{bucket_configuration.endpoint}/views/{url_encode(bucket_configuration.organisation)}" \
          f"/{url_encode(bucket_configuration.project)}/{url_encode(es_view_id)}?rev={rev}"

    original_payload_keys = ["@id", "@type", "mapping", "pipeline"]
    view_body = dict((k, view_body[k]) for k in original_payload_keys if k in view_body)
    view_body["resourceTag"] = resource_tag

    return check_response(requests.put(url=url, headers=make_header(token), json=view_body))


def create_es_view_legacy_params(
        bucket_configuration: NexusBucketConfiguration,
        token: str,
        es_view_id: str,
        mapping: Optional[Dict] = None,
        resource_tag: str = None,
        resource_types: Optional[List] = None,
        resource_schemas: Optional[List] = None,
        select_predicates: Optional[List] = None,
        default_label_predicates: Optional[bool] = False,
        source_as_text: Optional[bool] = False,
        include_metadata: bool = True,
        filter_deprecated: bool = True,
        construct_query: Optional[str] = None
) -> Dict:
    # TODO enable users to specify priority order in the pipeline for each param
    def build_pipeline():

        pipeline = []

        if filter_deprecated:
            pipeline.append({"name": "filterDeprecated"})

        if resource_schemas:
            pipeline.append({
                "name": "filterBySchema",
                "config": {
                    "types": resource_schemas
                }
            })

        if not include_metadata:
            pipeline.append({"name": "discardMetadata"})

        if default_label_predicates:
            pipeline.append({"name": "defaultLabelPredicates"})

        if source_as_text:
            pipeline.append({"name": "sourceAsText"})

        if resource_types:
            pipeline.append({
                "name": "filterByType",
                "config": {"types": resource_types}
            })
        if construct_query is not None:
            pipeline.append({
                "name": "dataConstructQuery",
                "config": {"query": construct_query}
            })
        if select_predicates:
            pipeline.append({
                "name": "selectPredicates",
                "config": {"predicates": select_predicates}
            })

        return pipeline

    return create_es_view(bucket_configuration=bucket_configuration, pipeline=build_pipeline(),
                          token=token, resource_tag=resource_tag, es_view_id=es_view_id,
                          mapping=mapping)
