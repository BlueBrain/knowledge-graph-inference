import os
from importlib.resources import Resource
from typing import List, Tuple, Dict, Optional

from kgforge.core import KnowledgeGraphForge

from inference_tools.bucket_configuration import NexusBucketConfiguration
from similarity_model.registration.model_registration_step import ModelRegistrationStep
from similarity_model.registration.logger import logger

from similarity_model.building.model_description import ModelDescription


from similarity_model.registration.helper_functions.embedding import (
    register_embeddings,
    load_embedding_model,
    get_embedding_vectors_from_pipeline
)
from similarity_model.constants import DST_DATA_DIR

from similarity_model.registration.common import check_forge_model
from similarity_model.registration.step import Step
from similarity_model.utils import get_path


def register_model_embeddings(
        model_description: ModelDescription,
        bucket_configuration: NexusBucketConfiguration,
        model_revision: Optional[str] = None,
        model: Optional[Resource] = None,
        forge: Optional[KnowledgeGraphForge] = None,
        resource_id_rev_list: Optional[List[Tuple[str, str]]] = None
        # if ever only a subset of the embeddings should be registered
):
    forge, model = check_forge_model(forge, model, model_description=model_description,
                                     bucket_configuration=bucket_configuration)

    model_id = model.id

    pipeline_directory = os.path.join(DST_DATA_DIR, "pipelines/")

    logger.info("2. Loading model in memory")
    model_revision, model_tag, pipeline = load_embedding_model(
        forge, model_revision=model_revision, download_dir=pipeline_directory, model_id=model_id
    )

    logger.info("3. Getting embedding vectors from model pipeline")
    missing_list, embedding_dict = get_embedding_vectors_from_pipeline(
        pipeline=pipeline, resource_id_rev_list=resource_id_rev_list
    )

    logger.info(f">  Number of missing embeddings in the embedding table: {len(missing_list)}")

    logger.info("4. Registering embeddings")
    register_embeddings(
        forge=forge, vectors=embedding_dict, model_revision=model_revision, model_id=model_id,
        embedding_tag=model_tag, mapping_path=get_path("./mappings/seu-embedding.hjson")
    )


registration_step_3 = ModelRegistrationStep(
    function_call=register_model_embeddings,
    step=Step.REGISTER_EMBEDDINGS,
    log_message="Registering embeddings"
)
