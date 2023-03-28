from typing import Dict

from inference_tools.helper_functions import get_type_attribute, get_id_attribute


class EmbeddingModel:

    org: str
    project: str
    type: str
    has_selector: Dict
    id: str

    def __init__(self, obj):
        self.org = obj.get("org", None)
        self.project = obj.get("project", None)
        self.type = get_type_attribute(obj)
        self.has_selector = obj.get("hasSelector", None)
        self.id = get_id_attribute(obj)
