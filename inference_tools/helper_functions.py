# This file is part of knowledge-graph-inference.
# Copyright 2024 Blue Brain Project / EPFL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Helper functions
"""
from typing import Dict, Type

from inference_tools.type import ObjectTypeStr, ObjectType

from inference_tools.exceptions.exceptions import (
    InferenceToolsException,
    IncompleteObjectException,
    InvalidValueException
)


def get_type_attribute(obj: Dict) -> str:
    """
    Looks into a dictionary for a type, located either at key "type" or "@type". Raises an error
    if the type is not present
    @param obj: the dictionary holding a type
    @type obj: Dict
    @return: the type value
    @rtype: str
    @raise TypeError
    """
    type_value = obj["type"] if "type" in obj else (obj["@type"] if "@type" in obj else None)
    if type_value:
        return type_value
    raise TypeError


def get_id_attribute(obj) -> str:
    """
        Looks into a dictionary for an id, located either at key "id" or "@id". Raises an error
        if the id is not present
        @param obj: the dictionary holding an id
        @type obj: Dict
        @return: the id value
        @rtype: str
        @raise TypeError
        """
    id_value = obj["id"] if "id" in obj else (obj["@id"] if "@id" in obj else None)
    if id_value:
        return id_value
    raise TypeError


def _follow_path(json_resource: Dict, path: str):
    """Follow a path in a JSON-resource."""
    value = json_resource
    path_list = path.split(".")

    for el in path_list:
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
