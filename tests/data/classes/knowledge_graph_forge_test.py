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

from kgforge.core import KnowledgeGraphForge
from typing import Optional, List, Union, Dict

from kgforge.core.wrappings.dict import DictWrapper

from tests.data.maps.elastic_data import elastic_patterns
from tests.data.maps.retrieve_data import retrieve_map

from tests.data.classes.resource_test import ResourceTest


class KnowledgeGraphForgeTest(KnowledgeGraphForge):

    def __init__(self, query_configuration_dict):
        self.bucket = f'{query_configuration_dict["org"]}/{query_configuration_dict["project"]}'
        self.endpoint = "https://bbp.epfl.ch/nexus/v1"

        self._store = DictWrapper({
            "bucket": self.bucket,
            "endpoint": self.endpoint,
            "service": DictWrapper({
                "sparql_endpoint":
                    {"endpoint": "_"},
                "elastic_endpoint":
                    {"endpoint": "_"}
            })
        })

        class Context:
            def __init__(self, bucket, endpoint):
                self.bucket = bucket
                self.endpoint = endpoint

            def expand(self, uri):
                if uri[0] != "<":
                    return f"{self.endpoint}/resources/{self.bucket}/_/{uri}"
                return uri

        class ModelTest:

            def __init__(self, bucket, endpoint):
                self.endpoint = endpoint
                self.bucket = bucket

            def context(self):
                return Context(self.bucket, self.endpoint)

        self._model = ModelTest(self.bucket, self.endpoint)

    def elastic(
            self,
            query: str,
            debug: bool = False,
            limit: Optional[int] = None,
            offset: Optional[int] = None,
            **params
    ) -> List[ResourceTest]:

        for pattern, res in elastic_patterns:
            matches = pattern(query, self.bucket)
            if matches:
                return [ResourceTest(e) for e in res]

        return []

    def sparql(
            self,
            query: str,
            debug: bool = False,
            limit: Optional[int] = None,
            offset: Optional[int] = None,
            **params
    ) -> List[ResourceTest]:
        return []

    def search(self, *filters, **params) -> List[ResourceTest]:
        return []

    def retrieve(
            self,
            id: str,
            version: Optional[Union[int, str]] = None,
            cross_bucket: bool = False,
            **params
    ) -> Optional[ResourceTest]:

        return ResourceTest(retrieve_map.get(id, None))

    def as_json(
            self,
            data: Union[ResourceTest, List[ResourceTest]],
            expanded: bool = False,
            store_metadata: bool = False,
    ) -> Union[Dict, List[Dict]]:
        if isinstance(data, list):
            return [e.__dict__ for e in data]

        return data.__dict__
