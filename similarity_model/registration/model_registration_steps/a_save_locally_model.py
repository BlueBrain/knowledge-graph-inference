import os
from typing import Optional

from similarity_model.registration.model_registration_step import ModelRegistrationStep
from similarity_model.registration.logger import logger
from similarity_model.building.model import Model

from similarity_model.building.model_description import ModelDescription

from similarity_model.building.model_data import ModelData

from similarity_model.constants import SRC_DATA_DIR, DST_DATA_DIR, PIPELINE_SUBDIRECTORY
from similarity_model.registration.step import Step


def save_locally_model(
        model_description: ModelDescription,
        model_data: Optional[ModelData],
        **kwargs
):

    logger.info("1. Initializing model")

    model_instance: Model = model_description.model(model_data)

    logger.info("2. Running model")

    pipeline = model_instance.run()
    pipeline_directory = os.path.join(DST_DATA_DIR, PIPELINE_SUBDIRECTORY)
    logger.info(f">  Saving {model_description.name} to {pipeline_directory}")

    os.makedirs(os.path.dirname(pipeline_directory), exist_ok=True)

    logger.info("3. Saving model")

    pipeline.save(os.path.join(pipeline_directory, model_description.filename), compress=True)


registration_step_1 = ModelRegistrationStep(
    function_call=save_locally_model,
    step=Step.SAVE_MODEL,
    log_message="Running and downloading locally"
)
