from inference_tools.helper_functions import get_type_attribute


class FragmentSelector:

    type: str
    conforms_to: str
    value: str

    def __init__(self, obj):
        self.type = get_type_attribute(obj)
        self.value = obj.get("value", None)
        self.conforms_to = obj.get("conformsTo", None)
