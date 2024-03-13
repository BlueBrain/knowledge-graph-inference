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

from typing import List, Union, Optional

from inference_tools.datatypes.parameter_specification import ParameterSpecification
from inference_tools.type import ObjectTypeStr, RuleType

from inference_tools.exceptions.exceptions import IncompleteObjectException

from inference_tools.datatypes.query_pipe import QueryPipe
from inference_tools.helper_functions import get_type_attribute, \
    get_id_attribute, _enforce_list
from inference_tools.datatypes.query import query_factory, premise_factory, Query


class Rule:

    name: str
    description: str
    search_query: Union[Query, QueryPipe]
    premises: Optional[List[Query]]
    id: str
    context: str
    type: List[RuleType]
    target_resource_type: str
    nexus_link: Optional[str]
    flattened_input_parameters: Optional[List[ParameterSpecification]] = None

    def __init__(self, obj):
        self.id = get_id_attribute(obj)
        self.name = obj.get("name", None)
        self.description = obj.get("description", None)
        self.type = [RuleType(e) for e in _enforce_list(get_type_attribute(obj))]

        self.context = obj.get("@context", None)
        self.target_resource_type = obj.get("targetResourceType", None)
        self.nexus_link = obj.get("nexus_link", None)

        tmp_premise = obj.get("premise", None)
        self.premises = [premise_factory(obj_i) for obj_i in _enforce_list(tmp_premise)] \
            if tmp_premise is not None else None

        tmp_sq = obj.get("searchQuery", None)
        if tmp_sq is None:
            raise IncompleteObjectException(
                name=self.name, attribute="searchQuery", object_type=ObjectTypeStr.RULE
            )

        self.search_query = query_factory(tmp_sq) \
            if get_type_attribute(tmp_sq) != "QueryPipe" else \
            QueryPipe(tmp_sq)

    def __repr__(self):
        id_str = f"Id: {self.id}"
        name_str = f"Name: {self.name}"
        desc_str = f"Description: {self.description}"
        type_str = f"Type: {[e.value for e in self.type]}"
        target_str = f"Target Resource Type: {self.target_resource_type}"
        nexus_link = f"Nexus link: {self.nexus_link}"
        context_str = f"Context: {self.context}"
        premises_str = f"Premises: {self.premises}"
        search_query_str = f"Search Query: {self.search_query}"

        return "\n".join([
            "Rule", id_str, name_str, desc_str, type_str, target_str, nexus_link, context_str,
            premises_str, search_query_str
        ]) + "\n"
