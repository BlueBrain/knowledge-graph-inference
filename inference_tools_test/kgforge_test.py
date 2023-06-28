from kgforge.core import KnowledgeGraphForge, Resource
from typing import Optional, List, Union, Dict

from kgforge.core.wrappings.dict import DictWrapper


class KnowledgeGraphForgeTest(KnowledgeGraphForge):
    def __init__(self):
        self._store = DictWrapper({
            "bucket": "a/b",
            "endpoint": "test",
            "service": DictWrapper({
                "sparql_endpoint":
                    {"endpoint": "idk"}
            })
        })

    def elastic(
            self,
            query: str,
            debug: bool = False,
            limit: Optional[int] = None,
            offset: Optional[int] = None,
    ) -> List[Resource]:
        return []

    def sparql(
            self,
            query: str,
            debug: bool = False,
            limit: Optional[int] = None,
            offset: Optional[int] = None,
            **params
    ) -> List[Resource]:
        return []

    def search(self, *filters, **params) -> List[Resource]:
        return []

    def as_json(
        self,
        data: Union[Resource, List[Resource]],
        expanded: bool = False,
        store_metadata: bool = False,
    ) -> Union[Dict, List[Dict]]:
        if isinstance(data, list):
            return []
        return {}
