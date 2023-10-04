from typing import Optional

from kgforge.core.wrappings.dict import DictWrapper
from numpy import random

from inference_tools_test.data.maps.id_data import (
    make_model_id, make_entity_id,
    make_embedding_id, make_org,
    make_project
)


def make_embedding(
        embedding_uuid, derivation_id, model_id, model_rev: int, entity_rev: int, bucket: str,
        vec_size: Optional[int] = 20, score: Optional[float] = None
) -> DictWrapper:
    entity_uuid = derivation_id.split("/")[-1]
    embedding_vec = [int(el) for el in list(random.randint(0, 100, size=vec_size))]

    temp = DictWrapper({
        "@context": "https://bbp.neuroshapes.org",
        "@id": make_embedding_id(embedding_uuid),
        "@type": [
            "Entity",
            "Embedding"
        ],
        "derivation": {
            "@type": "Derivation",
            "entity": {
                "@id": derivation_id,
                "@type": "Entity",
                "_rev": entity_rev
            }
        },
        "embedding": embedding_vec,
        "generation": {
            "@type": "Generation",
            "activity": {
                "@type": [
                    "Activity",
                    "EmbeddingActivity"
                ],
                "used": {
                    "@id": model_id,
                    "@type": "EmbeddingModel",
                    "_rev": model_rev
                },
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
        "name": [
            f"Embedding of {entity_uuid} at revision 40"
        ],
        "bucket": bucket
    })

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
            bucket=bucket
        ),
        [
            make_embedding(
                embedding_uuid=f"{embedding_uuid}{res_uuid}",
                derivation_id=make_entity_id(int(f"{embedding_uuid}{res_uuid}")),
                model_id=make_model_id(model_uuid),
                model_rev=model_rev,
                entity_rev=entity_rev,
                bucket=bucket,
                score=random.random()
            )
            for res_uuid in range(0, 10)
        ]
    )
    for embedding_uuid in range(0, 10)
]


def build_get_embedding_vector_query(embedding):
    get_embedding_vector_query = """{"from": 0, "size": 1, "query": {"bool": {"must": [{"nested": {
    "path": "derivation.entity", "query": {"term": {"derivation.entity.@id": 
    "$EMBEDDING_ID"}}}}]}}}""".replace("\n", "").replace("\t", "").replace("    ", "")

    derivation_id = embedding.__dict__["derivation"]["entity"]["@id"]
    embedding_bucket = embedding.__dict__["bucket"]

    def eq_check(query, bucket):
        full_q = get_embedding_vector_query.replace("$EMBEDDING_ID", derivation_id)
        a = query == full_q
        b = bucket == embedding_bucket
        return a and b

    return eq_check


def build_get_neighbor_query(embedding):
    get_neighbors_query = """{"from": 0, "size": 20, "query": {"script_score": {"query": {"bool": {
    "must_not": {"term": {"@id": "$EMBEDDING_ID"}}, "must": {"exists": {"field": "embedding"}}}}, 
    "script": {"source": 
    "if (doc['embedding'].size() == 0) { return 0; } double d = l2norm(params.query_vector, 
    'embedding'); return (1 / (1 + d))", "params": {"query_vector": [$QUERY_VECTOR]}}}}}""".\
        replace("\n", "").replace("\t", "").replace("    ", "")

    id_ = embedding.__dict__["@id"]
    vec = ", ".join([str(e) for e in embedding.__dict__["embedding"]])
    embedding_bucket = embedding.__dict__["bucket"]

    def eq_check(query, bucket):
        full_q = get_neighbors_query.replace("$EMBEDDING_ID", id_).replace("$QUERY_VECTOR", vec)
        return query == full_q and bucket == embedding_bucket

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
