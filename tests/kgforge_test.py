
from kgforge.core import KnowledgeGraphForge, Resource
from typing import Optional, List, Union, Dict


class KnowledgeGraphForgeTest(KnowledgeGraphForge):

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


