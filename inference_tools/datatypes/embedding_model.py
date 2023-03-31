from inference_tools.datatypes.fragment_selector import FragmentSelector
from inference_tools.helper_functions import get_type_attribute, get_id_attribute


class EmbeddingModel:

    org: str
    project: str
    type: str
    has_selector: FragmentSelector
    id: str

    def __init__(self, obj):
        self.org = obj.get("org", None)
        self.project = obj.get("project", None)
        self.type = get_type_attribute(obj)
        tmp_hs = obj.get("hasSelector", None)
        self.has_selector = FragmentSelector(tmp_hs) if tmp_hs is not None else None
        self.id = get_id_attribute(obj)
