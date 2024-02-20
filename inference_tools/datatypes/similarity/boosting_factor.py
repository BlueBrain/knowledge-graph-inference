from inference_tools.helper_functions import _enforce_list
from inference_tools.similarity.queries.common import _find_derivation_id


class BoostingFactor:
    entity_id: str
    value: int

    def __init__(self, obj):
        self.entity_id = _find_derivation_id(
            derivation_field=_enforce_list(obj["derivation"]), type_="Embedding"
        )
        self.value = obj["value"]
