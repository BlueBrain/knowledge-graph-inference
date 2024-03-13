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

from typing import Optional, List

from kgforge.core.wrappings.dict import DictWrapper
from numpy import random

from inference_tools.helper_functions import _enforce_list
from tests.data.maps.id_data import (
    make_model_id, make_entity_id,
    make_embedding_id, make_org,
    make_project
)


def make_derivation_entity(entity_id, entity_type):

    res = DictWrapper({
        "@context": "https://bbp.neuroshapes.org",
        "id": entity_id,
        "type": ["Entity"] + _enforce_list(entity_type),
        "name": "Test resource tied to embedding",
    })

    return res


def make_embedding(
        embedding_uuid,
        derivation_id,
        model_id,
        model_rev: int,
        entity_rev: int,
        embedding_vec: List[int],
        bucket: Optional[str] = None,
        score: Optional[float] = None,
        entity_type: str = "Entity"
) -> DictWrapper:
    entity_uuid = derivation_id.split("/")[-1]

    temp = DictWrapper({
        "@context": "https://bbp.neuroshapes.org",
        "@id": make_embedding_id(embedding_uuid),
        "@type": [
            "Entity",
            "nsg:Embedding"
        ],
        "derivation": [
            {
                "@type": "Derivation",
                "entity": {
                    "@id": derivation_id,
                    "@type": entity_type,
                    "_rev": entity_rev
                }
            },
            {
                "@type": "Derivation",
                "entity":  {
                    "@id": model_id,
                    "@type": "EmbeddingModel",
                    "_rev": model_rev
                }
            }
        ],
        "embedding": embedding_vec,
        "generation": {
            "@type": "Generation",
            "activity": {
                "@type": [
                    "Activity",
                    "EmbeddingActivity"
                ],
                "used": [
                    {
                        "@id": model_id,
                        "@type": "EmbeddingModel",
                        "_rev": model_rev
                    },
                    {
                        "@id": derivation_id,
                        "@type": entity_type,
                        "_rev": entity_rev
                    }
                ],
                "wasAssociatedWith": {
                    "@type": "SoftwareAgent",
                    "description":
                        "Unifying Python framework for graph analytics and co-occurrence analysis.",
                    "name": "BlueGraph",
                    "softwareSourceCode": {
                        "@type": "SoftwareSourceCode",
                        "codeRepository": "https://github.com/BlueBrain/BlueGraph",
                        "programmingLanguage": "Python",
                        "runtimePlatform": 3.7,
                        "version": "v0.1.2"
                    }
                }
            }
        },
        "name":  f"Embedding of {entity_uuid} at revision {entity_rev}"
    })

    if bucket is not None:
        temp.__dict__["bucket"] = bucket

    if score is not None:
        temp.__dict__["_store_metadata"] = DictWrapper({"_score": score})

    return temp


model_uuid, model_rev, entity_rev, bucket = 1, 13, 40, f"{make_org(1)}/{make_project(1)}"

embeddings = [
    (
        make_embedding(
            embedding_uuid=embedding_uuid,
            derivation_id=make_entity_id(embedding_uuid),
            model_id=make_model_id(model_uuid),
            model_rev=model_rev,
            entity_rev=entity_rev,
            bucket=bucket,
            embedding_vec=[int(el) for el in list(random.randint(0, 100, size=20))]
        ),
        [
            make_embedding(
                embedding_uuid=f"{embedding_uuid}{res_uuid}",
                derivation_id=make_entity_id(int(f"{embedding_uuid}{res_uuid}")),
                model_id=make_model_id(model_uuid),
                model_rev=model_rev,
                entity_rev=entity_rev,
                bucket=bucket,
                score=random.random(),
                embedding_vec=[int(el) for el in list(random.randint(0, 100, size=20))]
            )
            for res_uuid in range(0, 10)
        ]
    )
    for embedding_uuid in range(0, 10)
]


def build_get_embedding_vector_query(embedding):
    get_embedding_vector_query = """{"from": 0, "size": 1, "query": {"bool": {"must": [{"nested": {
    "path": "derivation.entity", "query": {"term": {"derivation.entity.@id": 
    "$EMBEDDING_ID"}}}}, {"term": {"_deprecated": false}}]}}}""".replace("\n", "").replace("\t", "").replace("    ", "")

    derivation_id = next(
        i["entity"]["@id"]
        for i in embedding.__dict__["derivation"]
        if i["entity"]["@type"] == "Entity"
    )
    embedding_bucket = embedding.__dict__["bucket"]

    def eq_check(query, bucket):
        full_q = get_embedding_vector_query.replace("$EMBEDDING_ID", derivation_id)
        a = query == full_q
        b = bucket == embedding_bucket
        return a and b

    return eq_check


def build_get_neighbor_query(embedding):
    get_neighbors_query = """{"from": 0, "size": 20, "query": {"script_score": {"query": {"bool": {
    "must_not": {"term": {"@id": "$EMBEDDING_ID"}}, "must": [{"exists": {"field": "embedding"}}]}}, 
    "script": {"source": 
    "if (doc['embedding'].size() == 0) { return 0; } double d = l2norm(params.query_vector, 
    'embedding'); return (1 / (1 + d))", "params": {"query_vector": [$QUERY_VECTOR]}}}}}""".\
        replace("\n", "").replace("\t", "").replace("    ", "")

    def eq_check(query, bucket):
        id_ = embedding.__dict__["@id"]
        vec = ", ".join([str(e) for e in embedding.__dict__["embedding"]])
        embedding_bucket = embedding.__dict__["bucket"]

        full_q = get_neighbors_query.replace("$EMBEDDING_ID", id_).replace("$QUERY_VECTOR", vec)
        a = query == full_q
        b = bucket == embedding_bucket
        return a and b

    return eq_check


query_embedding_patterns = [
    (build_get_embedding_vector_query(embedding), [embedding])
    for embedding, _ in embeddings
]

query_neighbors_patterns = [
    (build_get_neighbor_query(embedding), res)
    for embedding, res in embeddings
]

elastic_patterns = query_embedding_patterns + query_neighbors_patterns
