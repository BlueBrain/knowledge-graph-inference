# This file is part of knowledge-graph-inference.
# Copyright 2024 Blue Brain Project / EPFL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Optional, Callable
from abc import ABC, abstractmethod

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.view import View
from inference_tools.type import ObjectTypeStr

from inference_tools.datatypes.embedding_model_data_catalog import EmbeddingModelDataCatalog
from inference_tools.exceptions.exceptions import IncompleteObjectException, \
    SimilaritySearchException


class QueryConfiguration(ABC):
    org: str
    project: str

    def __init__(self, obj):
        if obj is None:
            raise IncompleteObjectException(
                object_type=ObjectTypeStr.QUERY, attribute="queryConfiguration"
            )

        self.org = obj.get("org", None)
        self.project = obj.get("project", None)

        if self.org is None:
            raise IncompleteObjectException(
                object_type=ObjectTypeStr.QUERY_CONFIGURATION, attribute="org"
            )

        if self.project is None:
            raise IncompleteObjectException(
                object_type=ObjectTypeStr.QUERY_CONFIGURATION, attribute="project"
            )

    @abstractmethod
    def use_factory(
            self,
            forge_factory: Callable[[str, str, Optional[str], Optional[str]],  KnowledgeGraphForge],
            sub_view: Optional[str] = None
    ) -> KnowledgeGraphForge:
        """
        Initialise a forge instance using the forge factory, and using the attributes of the
        QueryConfiguration instance as the necessary parameters of the forge factory:
        org, project, optional elastic search view and optional sparql view.
        Additionally, if a sub_view is specified, it will be used instead of the ES/Sparql view
        specified in this instance's. Whether the view will be used as a sparql or ES view
        is up to the QueryConfiguration specialisation.

        @param forge_factory: the factory to call to create a forge instance
        @type forge_factory: Callable[[str, str, Optional[str], Optional[str]]
        @param sub_view:
        @type sub_view: Optional[str]
        @return: a forge instance
        @rtype: KnowledgeGraphForge
        """

    def get_bucket(self):
        """
        Get bucket the query configuration is tied to
        @return: the bucket string, containing the organisation and project
        @rtype: str
        """
        return f"{self.org}/{self.project}"


class ForgeQueryConfiguration(QueryConfiguration):
    def use_factory(
            self,
            forge_factory: Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge],
            sub_view: Optional[str] = None
    ) -> KnowledgeGraphForge:
        return forge_factory(self.org, self.project, None, None)


class SparqlQueryConfiguration(QueryConfiguration):
    sparql_view: Optional[View]

    def __init__(self, obj):
        super().__init__(obj)
        tmp_sv = obj.get("sparqlView", None)
        self.sparql_view = View(tmp_sv) if tmp_sv is not None else None

    def __repr__(self):
        return f"Sparql Query Configuration: {self.sparql_view}"

    def use_factory(
            self,
            forge_factory: Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge],
            sub_view: Optional[str] = None
    ) -> KnowledgeGraphForge:

        return forge_factory(
            self.org, self.project, None,
            self.sparql_view.id if self.sparql_view is not None else None
        )


class ElasticSearchQueryConfiguration(QueryConfiguration):
    elastic_search_view: Optional[View]

    def __init__(self, obj):
        super().__init__(obj)
        tmp_esv = obj.get("elasticSearchView", None)
        self.elastic_search_view = View(tmp_esv) if tmp_esv is not None else None

    def __repr__(self):
        return f"ES Query Configuration: {self.elastic_search_view}"

    def use_factory(
            self,
            forge_factory: Callable[[str, str, Optional[str], Optional[str]], KnowledgeGraphForge],
            sub_view: Optional[str] = None
    ) -> KnowledgeGraphForge:

        return forge_factory(
            self.org, self.project,
            self.elastic_search_view.id if self.elastic_search_view is not None else None,
            None
        )


class SimilaritySearchQueryConfiguration(QueryConfiguration):
    embedding_model_data_catalog: EmbeddingModelDataCatalog
    similarity_view: View
    boosting_view: View
    statistics_view: View
    boosted: bool

    def __init__(self, obj):
        super().__init__(obj)
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
            sub_view: Optional[str] = None
    ) -> KnowledgeGraphForge:
        if sub_view == "similarity":
            return forge_factory(
                self.org, self.project, self.similarity_view.id, None
            )
        if sub_view == "boosting":
            return forge_factory(
                self.org, self.project, self.boosting_view.id, None
            )
        if sub_view == "statistic":
            return forge_factory(
                self.org, self.project, self.statistics_view.id, None
            )
        if sub_view is None:
            return forge_factory(
                self.org, self.project, None, None
            )
        raise SimilaritySearchException("Unknown view type for forge initialization")
