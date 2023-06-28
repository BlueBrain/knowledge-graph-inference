from dataclasses import dataclass

from kgforge.core import KnowledgeGraphForge, Resource
from typing import Optional, List, Union, Dict

from kgforge.core.wrappings.dict import DictWrapper

from data.similarity_search_data import test_embedding


class ResourceTest(Resource):

    def __init__(self, dict_):
        self.__dict__.update(dict_)


class KnowledgeGraphForgeTest(KnowledgeGraphForge):

    e = """{"from": 0, "size": 1, "query": {"bool": {"must": [{"nested": {"path": "derivation.entity", "query": {"terms": {"derivation.entity.@id": []}}}}]}}}"""

    elastic_patterns = [
        (lambda q: len(q) > 10 and q[50] == KnowledgeGraphForgeTest.e[50], [ResourceTest(
            DictWrapper(test_embedding))])
    ]

    def __init__(self):
        self._store = DictWrapper({
            "bucket": "a/b",
            "endpoint": "test",
            "service": DictWrapper({
                "sparql_endpoint":
                    {"endpoint": "idk"},
                "elastic_endpoint":
                    {"endpoint": "idk"}
            })
        })

    def elastic(
            self,
            query: str,
            debug: bool = False,
            limit: Optional[int] = None,
            offset: Optional[int] = None,
    ) -> List[ResourceTest]:

        for pattern, res in KnowledgeGraphForgeTest.elastic_patterns:
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
    ) -> ResourceTest:
        return ResourceTest(DictWrapper({"similarity": "euclidean"}))

        # pass  # TODO model retrieve is being done

    def as_json(
            self,
            data: Union[ResourceTest, List[ResourceTest]],
            expanded: bool = False,
            store_metadata: bool = False,
    ) -> Union[Dict, List[Dict]]:
        if isinstance(data, list):
            return [e.__dict__ for e in data]

        return data.__dict__
