from typing import Optional

from inference_tools.helper_functions import get_id_attribute, get_type_attribute


class View:
    id: str
    type: Optional[str]

    def __init__(self, obj):
        self.id = get_id_attribute(obj)
        try:
            self.type = get_type_attribute(obj)
        except TypeError:
            self.type = None

    def __repr__(self):
        return f"{self.type}: {self.id}"
