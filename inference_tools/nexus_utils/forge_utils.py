from typing import Tuple

from kgforge.core import KnowledgeGraphForge
from urllib.parse import quote_plus


class ForgeUtils:

    @staticmethod
    def _make_search_endpoint(
            endpoint: str, org: str, project: str, view: str, search_endpoint_suffix: str
    ):
        return "/".join((
            endpoint,
            "views",
            quote_plus(org),
            quote_plus(project),
            quote_plus(view),
            search_endpoint_suffix
        ))

    @staticmethod
    def make_sparql_endpoint(endpoint: str, org: str, project: str, view: str):
        return ForgeUtils._make_search_endpoint(endpoint, org, project, view, "sparql")

    @staticmethod
    def make_elastic_search_endpoint(endpoint: str, org: str, project: str, view: str):
        return ForgeUtils._make_search_endpoint(endpoint, org, project, view, "_search")

    @staticmethod
    def set_elastic_search_view(forge: KnowledgeGraphForge, view: str):
        endpoint, org, project = ForgeUtils.get_endpoint_org_project(forge)

        ForgeUtils.set_elastic_search_endpoint(
            forge,
            ForgeUtils.make_elastic_search_endpoint(endpoint, org, project, view)
        )

    @staticmethod
    def set_sparql_view(forge: KnowledgeGraphForge, view):
        """Set sparql view."""
        endpoint, org, project = ForgeUtils.get_endpoint_org_project(forge)

        ForgeUtils.set_sparql_endpoint(
            forge,
            ForgeUtils.make_sparql_endpoint(endpoint, org, project, view)
        )

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
    def get_endpoint_org_project(forge: KnowledgeGraphForge) -> Tuple[str, str, str]:
        store = ForgeUtils.get_store(forge)
        org, project = store.bucket.split("/")[-2:]
        return store.endpoint, org, project

    @staticmethod
    def get_token(forge: KnowledgeGraphForge) -> str:
        return ForgeUtils.get_store(forge).token

    @staticmethod
    def get_model(forge: KnowledgeGraphForge):
        return forge._model

    @staticmethod
    def expand_uri(forge: KnowledgeGraphForge, uri: str):
        return ForgeUtils.get_model(forge).context().expand(uri)

    @staticmethod
    def shrink_uri(forge: KnowledgeGraphForge, uri: str):
        return ForgeUtils.get_model(forge).context().shrink(uri)

    @staticmethod
    def to_symbol(forge: KnowledgeGraphForge, uri: str):
        return ForgeUtils.get_model(forge).context().to_symbol(uri)
