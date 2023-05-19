from typing import Tuple

from kgforge.core import KnowledgeGraphForge
from urllib.parse import quote_plus


class ForgeUtils:
    @staticmethod
    def set_sparql_view(forge: KnowledgeGraphForge, view):
        """Set sparql view."""
        endpoint, org, project = ForgeUtils.get_org_proj_endpoint(forge)
        views_endpoint = "/".join((
            endpoint,
            "views",
            quote_plus(org),
            quote_plus(project)
        ))

        endpoint = "/".join((views_endpoint, quote_plus(view), "sparql"))
        ForgeUtils.set_sparql_endpoint(forge, endpoint)

    @staticmethod
    def set_elastic_search_view(forge: KnowledgeGraphForge, view: str):
        endpoint, org, project = ForgeUtils.get_org_proj_endpoint(forge)

        views_endpoint = "/".join((
            endpoint,
            "views",
            quote_plus(org),
            quote_plus(project)
        ))

        endpoint = "/".join((views_endpoint, quote_plus(view), "_search"))
        ForgeUtils.set_elastic_search_endpoint(forge, endpoint)

    @staticmethod
    def get_store(forge: KnowledgeGraphForge):
        return forge._store

    @staticmethod
    def get_sparql_endpoint(forge: KnowledgeGraphForge) -> str:
        return ForgeUtils.get_store(forge).service.sparql_endpoint["endpoint"]

    @staticmethod
    def set_sparql_endpoint(forge: KnowledgeGraphForge, endpoint: str):
        ForgeUtils.get_store(forge).service.sparql_endpoint["endpoint"] = endpoint

    @staticmethod
    def get_elastic_search_endpoint(forge: KnowledgeGraphForge) -> str:
        return ForgeUtils.get_store(forge).service.elastic_endpoint["endpoint"]

    @staticmethod
    def set_elastic_search_endpoint(forge: KnowledgeGraphForge, endpoint: str):
        ForgeUtils.get_store(forge).service.elastic_endpoint["endpoint"] = endpoint

    @staticmethod
    def get_org_proj_endpoint(forge: KnowledgeGraphForge) -> Tuple[str, str, str]:
        store = ForgeUtils.get_store(forge)
        org, project = store.bucket.split("/")[-2:]
        return store.endpoint, org, project

    @staticmethod
    def get_token(forge: KnowledgeGraphForge) -> str:
        return ForgeUtils.get_store(forge).token
