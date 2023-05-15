from inference_tools.helper_functions import get_id_attribute


class Neighbor:

    entity_id: str

    def __init__(self, obj):
        self.entity_id = get_id_attribute(obj["derivation"]["entity"])
