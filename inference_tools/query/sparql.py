from urllib.parse import quote_plus

from string import Template


def set_sparql_view(forge, view):
    """Set sparql view."""
    views_endpoint = "/".join((
        forge._store.endpoint,
        "views",
        quote_plus(forge._store.bucket.split("/")[0]),
        quote_plus(forge._store.bucket.split("/")[1])))
    forge._store.service.sparql_endpoint["endpoint"] = "/".join(
        (views_endpoint, quote_plus(view), "sparql"))


def execute_sparql_query(forge, query, parameters, custom_sparql_view=None, debug=False):
    if custom_sparql_view is not None:
        view_id = (
            custom_sparql_view.get("id")
            if custom_sparql_view.get("id")
            else custom_sparql_view.get("@id")
        )
        set_sparql_view(forge, view_id)
    query = Template(
        query["hasBody"]).substitute(**parameters)
    return forge.as_json(forge.sparql(query, limit=None, debug=debug))


def check_sparql_premise(forge, query, parameters, custom_sparql_view=None, debug=False):
    results = execute_sparql_query(
        forge, query, parameters, custom_sparql_view, debug=debug)
    if len(results) > 0:
        return True
    return False
