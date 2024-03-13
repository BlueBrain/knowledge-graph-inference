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

import getpass
from typing import Optional

from kgforge.core import Resource, KnowledgeGraphForge

from data.maps.elastic_data import make_embedding, make_derivation_entity
from data.maps.id_data import make_model_id, make_entity_id
import random

from integration.conftest import init_forge


def process(forge: KnowledgeGraphForge, resource: Resource, tag: Optional[str]):

    forge.register(resource)

    last_action = resource._last_action

    if last_action.succeeded:

        if tag is not None:
            forge.tag(resource, tag)

    elif " already exists in project" in last_action.message:

        print(f"{resource.get_identifier()} already exists, updating...")

        existing_resource = forge.retrieve(resource.get_identifier())
        resource._store_metadata = existing_resource._store_metadata
        forge.update(resource)

        if tag is not None:
            forge.tag(resource, tag)


def generate(token):
    model_uuid, model_rev = 1, 1

    embedding_size = 10
    forge_test = init_forge(token=token, org="dke", project="test")

    def make_embedding_local(derivation_uuid, entity_type):
        emb = make_embedding(
            embedding_uuid=f"{model_uuid}{derivation_uuid}",
            derivation_id=make_entity_id(derivation_uuid),
            model_id=make_model_id(model_uuid),
            model_rev=model_rev,
            entity_rev=1,
            entity_type=entity_type,
            embedding_vec=[random.randint(0, 100) for _ in range(embedding_size)]
        )
        emb["type"] = emb["@type"]
        del emb["@type"]

        emb["id"] = emb["@id"]
        del emb["@id"]

        return emb

    def make_type(i):
        t = "Type1" if i < 6 else "Type2"
        return [t, "FakeType"]

    embeddings = [
        forge_test.from_json(
            make_embedding_local(derivation_uuid=i, entity_type=make_type(i))
        )
        for i in range(1, 10)
    ]

    derivations = [
        forge_test.from_json(
            make_derivation_entity(entity_type=make_type(i), entity_id=make_entity_id(i))
        )
        for i in range(1, 10)
    ]

    forge_test.validate(derivations, type_="Entity")
    forge_test.validate(embeddings, type_="Entity")

    tag_str = "test_data"

    for el in embeddings:
        process(forge_test, el, tag_str)

    for el in derivations:
        process(forge_test, el, None)


if __name__ == '__main__':
    generate(getpass.getpass(prompt="Production token"))

