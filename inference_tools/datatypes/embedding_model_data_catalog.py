from typing import List

from inference_tools.helper_functions import get_type_attribute, get_id_attribute
from inference_tools.similarity.formula import Formula


class EmbeddingModel:
    id: str
    rev: str

    def __init__(self, obj):
        self.id = get_id_attribute(obj)
        self.rev = obj["_rev"]


class EmbeddingModelDataCatalog:

    org: str
    project: str
    hasPart: List[EmbeddingModel]
    type: str
    id: str
    distance: Formula
    description: str

    def __init__(self, obj):
        self.org = obj.get("org", None)
        self.project = obj.get("project", None)
        self.name = obj.get("name", None)
        self.type = get_type_attribute(obj)
        self.id = get_id_attribute(obj)
        self.description = obj.get("description", None)

        t = obj.get("hasPart", None)
        self.hasPart = [EmbeddingModel(e) for e in t] if t is not None else []
        # TODO more processing?

        tmp_d = obj.get("distance", None)
        try:
            self.distance = Formula(tmp_d)
        except Exception as e:  # TODO find the specific exception type
            print(f"Invalid distance {tmp_d}")


