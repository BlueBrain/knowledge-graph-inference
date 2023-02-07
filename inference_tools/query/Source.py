
from abc import ABC, abstractmethod


class Source(ABC):

    @staticmethod
    @abstractmethod
    def execute_query(forge, query, parameters, config, debug=False):
        pass

    @staticmethod
    @abstractmethod
    def check_premise(forge, premise, parameters, config, debug=False):
        pass

    @staticmethod
    @abstractmethod
    def restore_default_views(forge):
        pass
