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

from typing import Union

from inference_tools.helper_functions import get_type_attribute
from inference_tools.datatypes.query import query_factory, Query


class QueryPipe:
    head: Query
    rest: Union[Query, 'QueryPipe']

    def __init__(self, obj):
        self.head = query_factory(obj["head"])
        tmp = obj["rest"]
        self.rest = query_factory(tmp) if get_type_attribute(tmp) != "QueryPipe" else \
            QueryPipe(tmp)

    def __repr__(self):
        return f"QueryPipe: \n\tHead: {self.head} \n\tRest: {self.rest}"
