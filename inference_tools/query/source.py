
from abc import ABC, abstractmethod
from kgforge.core import KnowledgeGraphForge


class Source(ABC):
    @staticmethod
    def get_store(forge: KnowledgeGraphForge):
        return forge._store

    @staticmethod
    @abstractmethod
    def execute_query(forge, query, parameters, config=None, debug=False):
        pass

    @staticmethod
    @abstractmethod
    def check_premise(forge, premise, parameters, config, debug=False):
        pass

    @staticmethod
    @abstractmethod
    def restore_default_views(forge):
        pass
