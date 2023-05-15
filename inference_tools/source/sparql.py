from typing import Dict
from urllib.parse import quote_plus

from string import Template

from kgforge.core import KnowledgeGraphForge

from inference_tools.type import ParameterType
from inference_tools.datatypes.query import SparqlQuery
from inference_tools.datatypes.query_configuration import SparqlQueryConfiguration
from inference_tools.premise_execution import PremiseExecution

from inference_tools.source.source import Source, DEFAULT_LIMIT

DEFAULT_SPARQL_VIEW = "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-sp/dataset"


# TODO get rid of the edit of views


class Sparql(Source):
    @staticmethod
    def execute_query(forge: KnowledgeGraphForge, query: SparqlQuery, parameter_values: Dict,
                      config: SparqlQueryConfiguration, limit=DEFAULT_LIMIT, debug: bool = False):

        query_body = query.body

        query_blocks = [x for x in query.parameter_specifications
                        if x.type == ParameterType.QUERY_BLOCK]

        if len(query_blocks) != 0:
            for qb in query_blocks:
                to_replace = f"${qb.name}"
                query_body = query.body.replace(to_replace, parameter_values[qb.name])

        if config.sparql_view is not None:
            Sparql.set_sparql_view(forge, config.sparql_view.id)

        query_body = Template(query_body).substitute(**parameter_values)

        return forge.sparql(query_body, limit=limit, debug=debug)

    @staticmethod
    def check_premise(forge: KnowledgeGraphForge, premise: SparqlQuery, parameter_values: Dict,
                      config: SparqlQueryConfiguration, debug: bool = False):

        results = Sparql.execute_query(forge=forge, query=premise,
                                       parameter_values=parameter_values,
                                       debug=debug, config=config, limit=None)

        return PremiseExecution.SUCCESS if results is not None and len(results) > 0 else \
            PremiseExecution.FAIL

    @staticmethod
    def set_sparql_view(forge: KnowledgeGraphForge, view):
        """Set sparql view."""
        bucket_configuration = Sparql.get_bucket_configuration(forge)

        views_endpoint = "/".join((
            bucket_configuration.endpoint,
            "views",
            quote_plus(bucket_configuration.organisation),
            quote_plus(bucket_configuration.project)
        ))

        Sparql.get_store(forge).service.sparql_endpoint["endpoint"] = "/".join(
            (views_endpoint, quote_plus(view), "sparql"))

    @staticmethod
    def restore_default_views(forge: KnowledgeGraphForge):
        Sparql.set_sparql_view(forge, DEFAULT_SPARQL_VIEW)
