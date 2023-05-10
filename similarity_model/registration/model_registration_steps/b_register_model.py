import os

from bluegraph import version as bg_version
from kgforge.core import Resource
from inference_tools.bucket_configuration import NexusBucketConfiguration

from similarity_model.allocate.allocate import allocate_forge_session_env
from similarity_model.registration.model_registration_step import ModelRegistrationStep
from similarity_model.registration.logger import logger
from similarity_model.building.model_description import ModelDescription
from similarity_model.registration.helper_functions.model import push_model
from similarity_model.constants import DST_DATA_DIR, PIPELINE_SUBDIRECTORY
from similarity_model.registration.step import Step


def register_model(
        model_description: ModelDescription,
        bucket_configuration: NexusBucketConfiguration,
        **kwargs
) -> Resource:

    pipeline_directory = os.path.join(DST_DATA_DIR, PIPELINE_SUBDIRECTORY)
    load_pipeline_path = os.path.join(pipeline_directory, f"{model_description.filename}.zip")

    logger.info(f">  Location: {bucket_configuration.organisation}/"
                f"{bucket_configuration.project} in"
                f" {'prod' if bucket_configuration.is_prod else 'staging'}")

    return push_model(
        forge=allocate_forge_session_env(bucket_configuration),
        model_description=model_description,
        description=model_description.description,
        pipeline_path=load_pipeline_path,
        distance_metric=model_description.distance,
        bluegraph_version=bg_version.__version__,
        label=model_description.label
    )


registration_step_2 = ModelRegistrationStep(
    function_call=register_model,
    step=Step.REGISTER_MODEL,
    log_message="Pushing to Nexus"
)

