from typing import List

from inference_tools.helper_functions import get_id_attribute


class Embedding:
    id: str
    vector: List[float]
    derivation_id: str

    def __init__(self, obj):
        self.id = get_id_attribute(obj)
        self.vector = obj["embedding"]
        self.derivation_id = obj["derivation"]
