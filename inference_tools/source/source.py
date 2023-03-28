from abc import ABC, abstractmethod
from typing import Dict

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.query import Query
from inference_tools.datatypes.query_configuration import QueryConfiguration

DEFAULT_LIMIT = 100


class Source(ABC):
    @staticmethod
    def get_store(forge: KnowledgeGraphForge):
        return forge._store

    @staticmethod
    @abstractmethod
    def execute_query(forge: KnowledgeGraphForge, query: Query, parameter_values: Dict,
                      config: QueryConfiguration, limit=DEFAULT_LIMIT, debug: bool = False):
        pass

    @staticmethod
    @abstractmethod
    def check_premise(forge: KnowledgeGraphForge, premise: Query, parameter_values: Dict,
                      config: QueryConfiguration, debug: bool = False):
        pass

    @staticmethod
    @abstractmethod
    def restore_default_views(forge: KnowledgeGraphForge):
        pass
