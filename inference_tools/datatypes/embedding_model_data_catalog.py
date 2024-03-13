# This file is part of knowledge-graph-inference.
# Copyright 2024 Blue Brain Project / EPFL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
