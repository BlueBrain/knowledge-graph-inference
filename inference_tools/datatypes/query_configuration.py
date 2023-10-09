from typing import Optional, NewType, Union

from inference_tools.datatypes.view import View
from inference_tools.type import ObjectTypeStr

from inference_tools.datatypes.embedding_model_data_catalog import EmbeddingModelDataCatalog
from inference_tools.exceptions.exceptions import IncompleteObjectException


class QueryConfigurationSuper:
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


class ForgeQueryConfiguration(QueryConfigurationSuper):
    ...


class SparqlQueryConfiguration(QueryConfigurationSuper):
    sparql_view: Optional[View]

    def __init__(self, obj, object_type):
        super().__init__(obj, object_type)
        tmp_sv = obj.get("sparqlView", None)
        self.sparql_view = View(tmp_sv) if tmp_sv is not None else None

    def __repr__(self):
        return f"Sparql Query Configuration: {self.sparql_view}"


class ElasticSearchQueryConfiguration(QueryConfigurationSuper):
    elastic_search_view: Optional[View]

    def __init__(self, obj, object_type):
        super().__init__(obj, object_type)
        tmp_esv = obj.get("elasticSearchView", None)
        self.elastic_search_view = View(tmp_esv) if tmp_esv is not None else None

    def __repr__(self):
        return f"ES Query Configuration: {self.elastic_search_view}"


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
        embedding_model_data_catalog_str = f"Embedding Model Data Catalog: " \
                                           f"{self.embedding_model_data_catalog}"

        return "\n".join([sim_view_str, boosted_str, boosting_view_str, stat_view_str,
                          embedding_model_data_catalog_str])


QueryConfiguration = NewType(
    "QueryConfiguration",
    Union[
        SparqlQueryConfiguration,
        ElasticSearchQueryConfiguration,
        SimilaritySearchQueryConfiguration
    ]
)
