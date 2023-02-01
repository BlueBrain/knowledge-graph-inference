from inference_tools.exceptions import InferenceToolsException


def _expand_uri(forge, uri):
    return forge._model.context().expand(uri)


def _safe_get_type(query):
    query_type = (
        query.get("type", None)
        if query.get("type", None)
        else query.get("@type", None)
    )
    if query_type is None:
        raise TypeError
    return query_type


def _follow_path(json_resource, path):
    """Follow a path in a JSON-resource."""
    value = json_resource
    path = path.split(".")

    for el in path:
        if el not in value:
            ex = InferenceToolsException(
                f"Invalid path for retrieving results: '{el}' is not in the path.")

            if el != "@id":
                raise ex

            if "id" in value:
                el = "id"
            else:
                raise ex

        value = value[el]
    return value


def _enforce_list(el):
    return el if isinstance(el, list) else [el]


def _enforce_unique(el):
    return el[0] if isinstance(el, list) else el
