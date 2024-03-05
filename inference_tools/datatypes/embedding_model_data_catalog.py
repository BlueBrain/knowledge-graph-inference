from typing import List

from inference_tools.helper_functions import get_type_attribute, get_id_attribute
from inference_tools.similarity.formula import Formula


class EmbeddingModel:
    id: str
    rev: str

    def __init__(self, obj):
        self.id = get_id_attribute(obj)
        self.rev = obj["_rev"]

    def __repr__(self):
        return f"Id: {self.id}\nRev: {self.rev}"


class EmbeddingModelDataCatalog:

    org: str
    project: str
    has_part: List[EmbeddingModel]
    type: str
    id: str
    distance: Formula
    name: str
    description: str
    about: str

    def __init__(self, obj):
        self.org = obj.get("org", None)
        self.project = obj.get("project", None)
        self.name = obj.get("name", None)
        self.type = get_type_attribute(obj)
        self.id = get_id_attribute(obj)
        self.description = obj.get("description", None)
        self.about = obj.get("about", None)

        t = obj.get("hasPart", None)
        self.has_part = [EmbeddingModel(e) for e in t] if t is not None else []
        # TODO more processing?

        tmp_d = obj.get("distance", None)
        try:
            self.distance = Formula(tmp_d)
        except ValueError:
            print(f"Invalid distance {tmp_d}")

    def __repr__(self):
        bucket_str = f"Bucket: {self.org}/{self.project}"
        name_str = f"Name: {self.name}"
        type_str = f"Type: {self.type}"
        id_str = f"Id: {self.id}"
        desc_str = f"Description: {self.description}"
        about_str = f"About: {self.about}"
        has_part_str = f"Has Part: {self.has_part}"

        return "\n".join(
            [id_str, name_str, type_str, bucket_str, desc_str, about_str, has_part_str]
        )
