from inference_tools.helper_functions import _enforce_list
from inference_tools.similarity.queries.common import _find_derivation_id


class BoostingFactor:
    entity_id: str
    value: int

    def __init__(self, obj):
        derivation = _enforce_list(obj["derivation"])
        self.entity_id = _find_derivation_id(derivation, type_="Embedding")
        self.value = obj["value"]
