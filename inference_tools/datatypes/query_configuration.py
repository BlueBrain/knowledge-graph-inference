from typing import Optional, NewType, Union, Callable

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.view import View
from inference_tools.type import ObjectTypeStr

from inference_tools.datatypes.embedding_model_data_catalog import EmbeddingModelDataCatalog
from inference_tools.exceptions.exceptions import IncompleteObjectException, \
    SimilaritySearchException

from abc import ABC, abstractmethod


class QueryConfigurationSuper(ABC):
    org: str
    project: str

    def __init__(self, obj, object_type: ObjectTypeStr):
        if obj is None:
            raise IncompleteObjectException(object_type=object_type, attribute="queryConfiguration")

        self.org = obj.get("org", None)
        self.project = obj.get("project", None)

        if self.org is None:
            raise IncompleteObjectException(object_type=ObjectTypeStr.QUERY_CONFIGURATION,
                                            attribute="org")

        if self.project is None:
            raise IncompleteObjectException(object_type=ObjectTypeStr.QUERY_CONFIGURATION,
                                            attribute="project")

    @abstractmethod
    def use_factory(
            self,
            forge_factory: Callable[[str, str, Optional[str], Optional[str]],  KnowledgeGraphForge],
            sub_view: str = None
    ) -> KnowledgeGraphForge:
        pass


class ForgeQueryConfiguration(QueryConfigurationSuper):
    def use_factory(
            self,
            forge_factory: Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge],
            sub_view: str = None
    ) -> KnowledgeGraphForge:
        return forge_factory(self.org, self.project, None, None)


class SparqlQueryConfiguration(QueryConfigurationSuper):
    sparql_view: Optional[View]

    def __init__(self, obj, object_type):
        super().__init__(obj, object_type)
        tmp_sv = obj.get("sparqlView", None)
        self.sparql_view = View(tmp_sv) if tmp_sv is not None else None

    def __repr__(self):
        return f"Sparql Query Configuration: {self.sparql_view}"

    def use_factory(
            self,
            forge_factory: Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge],
            sub_view: str = None
    ) -> KnowledgeGraphForge:

        return forge_factory(
            self.org, self.project, None,
            self.sparql_view.id if self.sparql_view is not None else None
        )


class ElasticSearchQueryConfiguration(QueryConfigurationSuper):
    elastic_search_view: Optional[View]

    def __init__(self, obj, object_type):
        super().__init__(obj, object_type)
        tmp_esv = obj.get("elasticSearchView", None)
        self.elastic_search_view = View(tmp_esv) if tmp_esv is not None else None

    def __repr__(self):
        return f"ES Query Configuration: {self.elastic_search_view}"

    def use_factory(
            self,
            forge_factory: Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge],
            sub_view: str = None
    ) -> KnowledgeGraphForge:

        return forge_factory(
            self.org, self.project,
            self.elastic_search_view.id if self.elastic_search_view is not None else None,
            None
        )


class SimilaritySearchQueryConfiguration(QueryConfigurationSuper):
    embedding_model_data_catalog: EmbeddingModelDataCatalog
    similarity_view: View
    boosting_view: View
    statistics_view: View
    boosted: bool

    def __init__(self, obj, object_type):
        super().__init__(obj, object_type)
        tmp_siv = obj.get("similarityView", None)
        self.similarity_view = View(tmp_siv) if tmp_siv is not None else None
        tmp_stv = obj.get("statisticsView", None)
        self.statistics_view = View(tmp_stv) if tmp_stv is not None else None
        tmp_bv = obj.get("boostingView", None)
        self.boosting_view = View(tmp_bv) if tmp_bv is not None else None
        tmp_em = obj.get("embeddingModelDataCatalog", None)
        self.embedding_model_data_catalog = EmbeddingModelDataCatalog(tmp_em) \
            if tmp_em is not None else None
        self.boosted = obj.get("boosted", False)

    def __repr__(self):
        sim_view_str = f"Similarity View: {self.similarity_view}"
        boosting_view_str = f"Boosting View: {self.boosting_view}"
        stat_view_str = f"Statistics View: {self.boosting_view}"
        boosted_str = f"Boosted: {self.boosted}"
        embedding_model_data_catalog_str = \
            f"Embedding Model Data Catalog: {self.embedding_model_data_catalog}"

        return "\n".join([sim_view_str, boosted_str, boosting_view_str, stat_view_str,
                          embedding_model_data_catalog_str])

    def use_factory(
            self,
            forge_factory: Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge],
            sub_view: str = None
    ) -> KnowledgeGraphForge:
        if sub_view == "similarity":
            return forge_factory(
                self.org, self.project, self.similarity_view.id, None
            )
        elif sub_view == "boosting":
            return forge_factory(
                self.org, self.project, self.boosting_view.id, None
            )
        elif sub_view == "statistic":
            return forge_factory(
                self.org, self.project, self.statistics_view.id, None
            )

        raise SimilaritySearchException("Unknown view type for forge initialization")


QueryConfiguration = NewType(
    "QueryConfiguration",
    Union[
        SparqlQueryConfiguration,
        ElasticSearchQueryConfiguration,
        SimilaritySearchQueryConfiguration
    ]
)
