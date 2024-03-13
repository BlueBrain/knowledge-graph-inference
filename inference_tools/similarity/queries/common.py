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

from typing import List

from inference_tools.exceptions.exceptions import SimilaritySearchException
from inference_tools.helper_functions import get_type_attribute, get_id_attribute


def _find_derivation_id(derivation_field: List, type_: str) -> str:
    """

    @param derivation_field: the derivation field of an embedding
    @type derivation_field: List
    @param type_: the type of the resource that is one of the derivations
    @type type_:
    @return: the id of the resource that is a derivation, and that has the specified type
    @rtype:
    """
    el = next(
        (
            e for e in derivation_field if type_ in get_type_attribute(e["entity"])
        ), None
    )
    if el is None:
        raise SimilaritySearchException(
            f"Couldn't find derivation of type {type_} within an embedding"
        )

    return get_id_attribute(el["entity"])
