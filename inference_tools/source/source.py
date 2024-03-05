from abc import ABC, abstractmethod
from typing import Dict

from kgforge.core import KnowledgeGraphForge


DEFAULT_LIMIT = 20


class Source(ABC):

    @staticmethod
    @abstractmethod
    def execute_query(
            forge: KnowledgeGraphForge, query, parameter_values: Dict,
            config, limit=DEFAULT_LIMIT, debug: bool = False
    ):
        pass

    @staticmethod
    @abstractmethod
    def check_premise(
            forge: KnowledgeGraphForge, premise, parameter_values: Dict,
            config, debug: bool = False
    ):
        pass

    @staticmethod
    @abstractmethod
    def restore_default_views(forge: KnowledgeGraphForge):
        pass
