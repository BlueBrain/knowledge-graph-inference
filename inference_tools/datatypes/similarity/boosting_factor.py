from inference_tools.helper_functions import get_id_attribute


class BoostingFactor:
    entity_id: str
    value: int

    def __init__(self, obj):
        self.entity_id = get_id_attribute(obj["derivation"]["entity"])
        self.value = obj["value"]
