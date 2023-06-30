from kgforge.core import KnowledgeGraphForge, Resource
from typing import Optional, List, Union, Dict

from kgforge.core.wrappings.dict import DictWrapper

from inference_tools_test.data.datamaps.elastic_data import elastic_patterns
from inference_tools_test.data.dataclasses.resource_test import ResourceTest
from inference_tools_test.data.datamaps.retrieve_data import retrieve_map


class KnowledgeGraphForgeTest(KnowledgeGraphForge):

    def __init__(self):
        self._store = DictWrapper({
            "bucket": "a/b",
            "endpoint": "test",
            "service": DictWrapper({
                "sparql_endpoint":
                    {"endpoint": "_"},
                "elastic_endpoint":
                    {"endpoint": "_"}
            })
        })

    def elastic(
            self,
            query: str,
            debug: bool = False,
            limit: Optional[int] = None,
            offset: Optional[int] = None,
    ) -> List[ResourceTest]:

        for pattern, res in elastic_patterns:
            if pattern(query):
                return res

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
        return retrieve_map.get(id, None)


    def as_json(
            self,
            data: Union[ResourceTest, List[ResourceTest]],
            expanded: bool = False,
            store_metadata: bool = False,
    ) -> Union[Dict, List[Dict]]:
        if isinstance(data, list):
            return [e.__dict__ for e in data]

        return data.__dict__
