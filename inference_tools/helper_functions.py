"""
Helper functions
"""

from inference_tools.exceptions import InferenceToolsException


def get_model(forge):
    return forge._model


def _expand_uri(forge, uri):
    return get_model(forge).context().expand(uri)


def _shrink_uri(forge, uri):
    return get_model(forge).context().shrink(uri)


def _to_symbol(forge, uri):
    return get_model(forge).context().to_symbol(uri)


def _safe_get_type_attribute(obj):
    type_value = obj["type"] if "type" in obj else (obj["@type"] if "@type" in obj else None)
    if type_value:
        return type_value
    raise TypeError


def _safe_get_id_attribute(obj):
    id_value = obj["id"] if "id" in obj else (obj["@id"] if "@id" in obj else None)
    if id_value:
        return id_value
    raise TypeError


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
