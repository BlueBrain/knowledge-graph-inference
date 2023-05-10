from abc import ABC, abstractmethod
from bluegraph.downstream import EmbeddingPipeline

from similarity_model.building.model_data import ModelData


class Model(ABC):
    @abstractmethod
    def __init__(self, model_data: ModelData):
        pass

    @abstractmethod
    def run(self) -> EmbeddingPipeline:
        pass
