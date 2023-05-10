import os
import uuid
from typing import List, Dict, Optional, Tuple

from bluegraph.core import GraphElementEmbedder
from bluegraph.downstream import EmbeddingPipeline
from kgforge.core import KnowledgeGraphForge, Resource
from kgforge.specializations.mappings import DictionaryMapping

from similarity_model.registration.logger import logger
from similarity_model.utils import encode_id_rev, get_model_tag, parse_id_rev


def load_embedding_model(forge: KnowledgeGraphForge, model_id: str,
                         model_revision: Optional[str] = None,
                         download_dir: str = ".") -> Tuple[str, str, EmbeddingPipeline]:
    """
    Load embedding models into memory
    @param forge:
    @type forge:
    @param model_id:
    @type model_id:
    @param model_revision:
    @type model_revision:
    @param download_dir:
    @type download_dir:
    @return:
    @rtype:
    """

    retrieval_str = f"{model_id}{'?rev='}{model_revision}" \
        if model_revision is not None else model_id

    model = forge.retrieve(retrieval_str)

    # If revision is not provided by the user, fetch the latest
    model_revision = model_revision if model_revision is not None else model._store_metadata._rev

    model_tag = get_model_tag(model_id, model_revision)

    forge.download(
        model, "distribution.contentUrl", download_dir, overwrite=True)

    path = os.path.join(download_dir, model.distribution.name)

    pipeline = EmbeddingPipeline.load(
        path=path, embedder_interface=GraphElementEmbedder, embedder_ext="zip"
    )

    return model_revision, model_tag, pipeline


def get_embedding_vectors_from_pipeline(pipeline: EmbeddingPipeline,
                                        resource_id_rev_list: Optional[List[Tuple[str, str]]]) \
        -> Tuple[
            List[Tuple[str, str]],  # missing embeddings
            Dict[Tuple[str, str], List[float]]  # embeddings found
        ]:
    """
    Get embedding vectors from an Embedding Pipeline
    @param pipeline:
    @type pipeline:
    @param resource_id_rev_list: a specific set of resource ids to get the embeddings from.
    If not specified, all embeddings in the embedding table of the pipeline will be returned
    @type resource_id_rev_list:

    @return: a dict with key: id, rev, value: Embedding vector
    @rtype:
    """

    embedding_table = pipeline.generate_embedding_table()

    if resource_id_rev_list is None:  # Get all of them
        tmp = embedding_table.loc[:, "embedding"].to_dict()
        tmp = dict((parse_id_rev(key), value.tolist()) for key, value in tmp.items())
        return [], tmp

    def get_from_embedding_table(resource_id, resource_rev) -> Optional[List[float]]:
        if resource_rev is not None:
            key = encode_id_rev(resource_id, resource_rev)
            try:
                return embedding_table.loc[key, "embedding"].tolist()
            except KeyError:
                return None
        else:
            return None
            # TODO if no rev is specified
            #  GET EMBEDDING FOR ANY REV -> iterate and if the split index
            #  equals the id then return the thing

    computation: Dict[Tuple[str, str], Optional[List[float]]] = dict(
        (
            (resource_id, resource_rev),
            get_from_embedding_table(resource_id, resource_rev)
        )
        for resource_id, resource_rev in resource_id_rev_list
    )

    missing = [key for key, value in computation.items() if value is None]
    existing = dict((key, value) for key, value in computation.items() if value is not None)

    return missing, existing


def register_embeddings(forge: KnowledgeGraphForge, vectors: Dict[Tuple[str, str], List[float]],
                        model_id: str, model_revision: str, embedding_tag: str,
                        mapping_path: str):
    """
    Register embedding vectors
    @param forge:
    @type forge: KnowledgeGraphForge
    @param vectors: a dictionary with keys the entity ids + rev and the values the associated
    embedding vectors
    @type vectors: Dict[str, List[int]]
    @param model_id: the id of the embedding model
    @type model_id: str
    @param model_revision: the revision of the embedding model
    @type model_revision: str
    @param embedding_tag:
    @type embedding_tag: str
    @param mapping_path: the path to a mapping indicating an embedding's format
    @type mapping_path: str
    """

    mapping = DictionaryMapping.load(mapping_path)

    def search_vector(entity_id: str, entity_rev: str) -> List[Resource]:
        return forge.search({
            "type": "Embedding",
            "derivation": {
                "entity": {
                    "id": entity_id,
                    "hasSelector": {
                        "value": f"?rev={entity_rev}"
                    }
                }
            },
            "generation": {
                "activity": {
                    "used": {
                        "id": model_id
                    }
                }
            }
        })

    def update(existing_vectors, embedding) -> Resource:
        embedding_vector_resource = existing_vectors[0]
        embedding_vector_resource.embedding = embedding
        embedding_vector_resource.generation.activity.used.hasSelector = \
            forge.from_json({
                "type": "FragmentSelector",
                "conformsTo":
                    "https://bluebrainnexus.io/docs/delta/api/resources-api.html#fetch",
                "value": f"?rev={model_revision}"
            })
        return embedding_vector_resource  # TODO check this is a res

    def create(entity_id, entity_rev, embedding) -> Resource:

        entity_uuid = entity_id.split('/')[-1]
        entity_dict = {
            "morphology_id": entity_id,
            "morphology_rev": entity_rev,
            "model_id": model_id,
            "model_rev": model_revision,
            "embedding_name": f"Embedding of {entity_uuid} at revision {entity_rev}",
            "embedding": embedding,
            "uuid": entity_uuid
        }
        entity_resource = forge.map(entity_dict, mapping)
        entity_resource.id = forge.format("identifier", "embeddings", str(uuid.uuid4()))
        return entity_resource

    new_embeddings: List[Resource] = []
    updated_embeddings: List[Resource] = []

    for (entity_id_i, entity_rev_i), embedding_vector_i in vectors.items():
        existing_vectors_i = search_vector(entity_id_i, entity_rev_i)
        # Embedding vector for this entity and this model exists, update it
        if existing_vectors_i is not None and len(existing_vectors_i) > 0:
            updated_embeddings.append(update(existing_vectors_i, embedding_vector_i))
        # Embedding vector for this entity and this model does not exist, create it
        else:
            new_embeddings.append(create(entity_id_i, entity_rev_i, embedding_vector_i))

    # new_embedding_resources: List[Resource] = forge.map(new_embeddings, mapping)
    # for r in new_embedding_resources:
        # new_embedding_resources = forge.from_json(
        #     forge.as_json(new_embedding_resources))
        # forge.register(new_embedding_resources)
        # for r in new_embedding_resources:
        #     forge.tag(forge.retrieve(r.id), tag)

    logger.info(">  Created embeddings: ", len(new_embeddings))
    forge.register(new_embeddings)

    logger.info(">  Updated embeddings: ", len(updated_embeddings))
    forge.update(updated_embeddings)

    created_embeddings = [forge.retrieve(r.id) for r in new_embeddings]

    logger.info(">  Tagging created embeddings...")
    forge.tag(created_embeddings, embedding_tag)

    logger.info(">  Tagging updated embeddings...")
    forge.tag(updated_embeddings, embedding_tag)

# Not used in data registration steps I believe - Population of Inference Test project
#  notebook seems to be the only usage ?
# def vectors_to_resources(forge: KnowledgeGraphForge, vectors, resource_ids: List[str],
#                          model_id: str) -> List[Resource]:  # vectors is a numpy ndarray
#
#     """
#     Create resources from vectors
#     @param forge:
#     @type forge: KnowledgeGraphForge
#     @param vectors:
#     @type vectors:
#     @param resource_ids: the ids of the resources that have been embedded into vectors
#     @type resource_ids: List[str]
#     @param model_id:
#     @type model_id: str
#     @return: The embedding vectors as resources
#     @rtype: List[Resource]
#     """
#
#     def vec_to_res(vector, resource):
#         return {
#             "@id": str(uuid.uuid4()),
#             "@type": ["Entity", "Embedding"],
#             "derivation": {
#                 "@type": "Derivation",
#                 "entity": {
#                     "@id": resource,
#                     "@type": "Entity"
#                 }
#             },
#             "embedding": vector.tolist(),
#             "generation": {
#                 "@type": "Generation",
#                 "activity": {
#                     "@type": [
#                         "Activity",
#                         "EmbeddingActivity"
#                     ],
#                     "used": {
#                         "@id": model_id,
#                         "@type": "EmbeddingModel",
#                     }
#                 }
#             },
#             "name": f"Embedding of {resource}"
#         }
#
#     return forge.from_json(
#         [vec_to_res(vector, resource) for resource, vector in zip(resource_ids, vectors)]
#     )
