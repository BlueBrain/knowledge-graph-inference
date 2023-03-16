from urllib.parse import quote_plus

from string import Template

from inference_tools.premise_execution import PremiseExecution

from inference_tools.query.source import Source
from inference_tools.helper_functions import _enforce_list, _safe_get_id_attribute

DEFAULT_SPARQL_VIEW = "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-sp/dataset"
# TODO get rid of the edit of views


class Sparql(Source):
    @staticmethod
    def execute_query(forge, query, parameters, config=None, debug=False):

        custom_sparql_view = config.get("sparqlView", None) if config else None

        if custom_sparql_view is not None:
            Sparql.set_sparql_view(forge, _safe_get_id_attribute(custom_sparql_view))

        query = Template(query["hasBody"]).substitute(**parameters)

        return forge.as_json(forge.sparql(query, limit=None, debug=debug))

    @staticmethod
    def check_premise(forge, premise, parameters, config, debug=False):

        results = Sparql.execute_query(
            forge, premise, parameters, config, debug=debug)

        return PremiseExecution.SUCCESS if len(results) > 0 else PremiseExecution.FAIL

    @staticmethod
    def set_sparql_view(forge, view):
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
    def restore_default_views(forge):
        forge = _enforce_list(forge)
        for f in forge:
            Sparql.set_sparql_view(f, DEFAULT_SPARQL_VIEW)
