from typing import Dict, List
from urllib.parse import quote_plus

from string import Template

from inference_tools.type import ParameterType
from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.query import SparqlQuery
from inference_tools.datatypes.query_configuration import SparqlQueryConfiguration
from inference_tools.premise_execution import PremiseExecution

from inference_tools.source.source import Source, DEFAULT_LIMIT
from inference_tools.helper_functions import _enforce_list, get_id_attribute

DEFAULT_SPARQL_VIEW = "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-sp/dataset"


# TODO get rid of the edit of views


class Sparql(Source):
    @staticmethod
    def execute_query(forge: KnowledgeGraphForge, query: SparqlQuery, parameter_values: Dict,
                      config: SparqlQueryConfiguration, limit=DEFAULT_LIMIT, debug: bool = False):

        query_body = query.body

        query_block = [x for x in query.parameter_specifications
                       if x.type == ParameterType.QUERY_BLOCK]

        if len(query_block) != 0:
            for qb in query_block:
                replace = f"${qb.name}"
                query_body = query.body.replace(replace, parameter_values[qb.name])

        if config.sparql_view is not None:
            Sparql.set_sparql_view(forge, config.sparql_view.id)

        query_body = Template(query_body).substitute(**parameter_values)

        return forge.as_json(forge.sparql(query_body, limit=limit, debug=debug))

    @staticmethod
    def check_premise(forge: KnowledgeGraphForge, premise: SparqlQuery, parameter_values: Dict,
                      config: SparqlQueryConfiguration, debug: bool = False):

        results = Sparql.execute_query(forge=forge, query=premise,
                                       parameter_values=parameter_values,
                                       debug=debug, config=config, limit=None)

        return PremiseExecution.SUCCESS if len(results) > 0 else PremiseExecution.FAIL

    @staticmethod
    def set_sparql_view(forge: KnowledgeGraphForge, view):
        """Set sparql view."""
        org, project = Sparql.get_store(forge).bucket.split("/")[-2:]
        views_endpoint = "/".join((
            Sparql.get_store(forge).endpoint,
            "views",
            quote_plus(org),
            quote_plus(project)
        ))

        Sparql.get_store(forge).service.sparql_endpoint["endpoint"] = "/".join(
            (views_endpoint, quote_plus(view), "sparql"))

    @staticmethod
    def restore_default_views(forge: KnowledgeGraphForge):
        Sparql.set_sparql_view(forge, DEFAULT_SPARQL_VIEW)
