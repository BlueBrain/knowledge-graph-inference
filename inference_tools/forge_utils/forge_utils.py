from kgforge.core import KnowledgeGraphForge
from inference_tools.bucket_configuration import NexusBucketConfiguration
from urllib.parse import quote_plus


def set_sparql_view(forge: KnowledgeGraphForge, view):
    """Set sparql view."""
    bucket_configuration = NexusBucketConfiguration.create_from_forge(forge)

    views_endpoint = "/".join((
        bucket_configuration.endpoint,
        "views",
        quote_plus(bucket_configuration.organisation),
        quote_plus(bucket_configuration.project)
    ))

    endpoint = "/".join((views_endpoint, quote_plus(view), "sparql"))
    set_sparql_endpoint(forge, endpoint)


def set_elastic_search_view(forge: KnowledgeGraphForge, view: str):
    bucket_configuration = NexusBucketConfiguration.create_from_forge(forge)

    views_endpoint = "/".join((
        bucket_configuration.endpoint,
        "views",
        quote_plus(bucket_configuration.organisation),
        quote_plus(bucket_configuration.project)
    ))

    endpoint = "/".join((views_endpoint, quote_plus(view), "_search"))
    set_elastic_search_endpoint(forge, endpoint)


def get_store(forge: KnowledgeGraphForge):
    return forge._store


def get_sparql_endpoint(forge: KnowledgeGraphForge):
    return get_store(forge).service.sparql_endpoint["endpoint"]


def set_sparql_endpoint(forge: KnowledgeGraphForge, endpoint: str):
    get_store(forge).service.sparql_endpoint["endpoint"] = endpoint


def get_elastic_search_endpoint(forge: KnowledgeGraphForge):
    return get_store(forge).service.elastic_endpoint["endpoint"]


def set_elastic_search_endpoint(forge: KnowledgeGraphForge, endpoint: str):
    get_store(forge).service.elastic_endpoint["endpoint"] = endpoint


