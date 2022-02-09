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
        (views_endpoint, quote_plus(view), "_search"))


def execute_sparql_query(forge, query, parameters, custom_sparql_view=None):
    if custom_sparql_view is not None:
        set_sparql_view(forge, custom_sparql_view["id"])

    query = Template(
        query["hasBody"]).substitute(**parameters)
    return forge.as_json(forge.sparql(query, limit=None))


def check_sparql_premise(forge, query, parameters, custom_sparql_view=None):
    results = execute_sparql_query(
        forge, query, parameters, custom_sparql_view)
    if len(results) > 0:
        return True
    return False
