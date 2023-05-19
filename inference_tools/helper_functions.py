"""
Helper functions
"""
from typing import Dict, Type
import os

from inference_tools.type import ObjectTypeStr, ObjectType

from inference_tools.exceptions.exceptions import (
    InferenceToolsException,
    IncompleteObjectException,
    InvalidValueException
)


def get_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_model(forge):
    return forge._model


def _expand_uri(forge, uri):
    return get_model(forge).context().expand(uri)


def _shrink_uri(forge, uri):
    return get_model(forge).context().shrink(uri)


def _to_symbol(forge, uri):
    return get_model(forge).context().to_symbol(uri)


def get_type_attribute(obj):
    type_value = obj["type"] if "type" in obj else (obj["@type"] if "@type" in obj else None)
    if type_value:
        return type_value
    raise TypeError


def get_id_attribute(obj) -> str:
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


def _get_type(obj: Dict, obj_type: ObjectTypeStr, type_type: Type[ObjectType]) -> ObjectType:
    """
    Gets a type from a dictionary, and converts this type to the appropriate enum
    @param obj: the dictionary holding a type field
    @type obj: Dict
    @param obj_type: the type of the dictionary
    @type obj_type: ObjectTypeStr
    @param type_type: the enum class for the type => the type of the type
    @type type_type Type[ObjectType]
    @return: an instance of type_type
    @rtype: ObjectType
    """
    try:
        type_value = get_type_attribute(obj)
    except TypeError as e:
        raise IncompleteObjectException(object_type=obj_type, attribute="type") from e

    try:
        return type_type(type_value)
    except ValueError as e:
        raise InvalidValueException(attribute=f"{obj_type.value} type", value=type_value) from e
