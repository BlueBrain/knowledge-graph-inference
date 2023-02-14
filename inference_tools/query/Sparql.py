from urllib.parse import quote_plus

from string import Template

from inference_tools.PremiseExecution import PremiseExecution

from inference_tools.query.Source import Source
from inference_tools.helper_functions import _enforce_list

DEFAULT_SPARQL_VIEW = "https://bluebrain.github.io/nexus/vocabulary/defaultSparqlIndex"


class Sparql(Source):

    @staticmethod
    def execute_query(forge, query, parameters, config=None, debug=False):
        custom_sparql_view = config.get("sparqlView", None) if config else None

        if custom_sparql_view is not None:
            view_id = (
                custom_sparql_view.get("id")
                if custom_sparql_view.get("id")
                else custom_sparql_view.get("@id")
            )
            Sparql.set_sparql_view(forge, view_id)
        query = Template(
            query["hasBody"]).substitute(**parameters)
        return forge.as_json(forge.sparql(query, limit=None, debug=debug))

    @staticmethod
    def check_premise(forge, premise, parameters, config, debug=False):
        custom_sparql_view = config.get("sparqlView", None)

        results = Sparql.execute_query(
            forge, premise, parameters, custom_sparql_view, debug=debug)

        return PremiseExecution.SUCCESS if len(results) > 0 else PremiseExecution.FAIL

    @staticmethod
    def set_sparql_view(forge, view):
        """Set sparql view."""
        views_endpoint = "/".join((
            forge._store.endpoint,
            "views",
            quote_plus(forge._store.bucket.split("/")[0]),
            quote_plus(forge._store.bucket.split("/")[1])))
        forge._store.service.sparql_endpoint["endpoint"] = "/".join(
            (views_endpoint, quote_plus(view), "sparql"))

    @staticmethod
    def restore_default_views(forge):
        forge = _enforce_list(forge)
        for f in forge:
            Sparql.set_sparql_view(f, DEFAULT_SPARQL_VIEW)
