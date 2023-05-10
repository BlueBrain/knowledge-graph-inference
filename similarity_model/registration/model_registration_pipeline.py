from typing import Tuple, List, Optional
from enum import Enum

from inference_tools.bucket_configuration import NexusBucketConfiguration
from similarity_model.allocate.allocate import allocate_forge_session_env
from similarity_model.building.model_data import ModelData
from similarity_model.building.model_data_impl.model_data_impl import ModelDataImpl
from similarity_model.building.model_description import ModelDescription
from similarity_model.constants import SRC_DATA_DIR, DST_DATA_DIR
from similarity_model.registration.logger import logger

from similarity_model.registration.model_registration_step import ModelRegistrationStep

from similarity_model.registration.model_registration_steps.a_save_locally_model \
    import registration_step_1
from similarity_model.registration.model_registration_steps.b_register_model \
    import registration_step_2
from similarity_model.registration.model_registration_steps.c_register_embeddings \
    import registration_step_3
from similarity_model.registration.model_registration_steps.d_register_similarity_view \
    import registration_step_4
from similarity_model.registration.model_registration_steps.e_register_non_boosted_stats \
    import registration_step_5
from similarity_model.registration.model_registration_steps.f_register_boosting_factors \
    import registration_step_6
from similarity_model.registration.model_registration_steps.g_register_boosting_view \
    import registration_step_7
from similarity_model.registration.model_registration_steps.h_register_boosted_stats \
    import registration_step_8
from similarity_model.registration.model_registration_steps.i_register_stats_view \
    import registration_step_9


class Step(Enum):
    SAVE_MODEL = 1
    REGISTER_MODEL = 2
    REGISTER_EMBEDDINGS = 3
    REGISTER_SIMILARITY_VIEW = 4
    REGISTER_NON_BOOSTED_STATS = 5
    REGISTER_BOOSTING_FACTORS = 6
    REGISTER_BOOSTING_VIEW = 7
    REGISTER_BOOSTED_STATS = 8
    REGISTER_STATS_VIEW = 9


class ModelRegistrationPipeline:
    steps: List[ModelRegistrationStep] = [
        registration_step_1,
        registration_step_2,
        registration_step_3,
        registration_step_4,
        registration_step_5,
        registration_step_6,
        registration_step_7,
        registration_step_8,
        registration_step_9
    ]

    @staticmethod
    def get_step(step: Step) -> ModelRegistrationStep:
        return ModelRegistrationPipeline.steps[step.value - 1]

    @staticmethod
    def run_from(included_models: List[Tuple[str, ModelDescription]],
                 bucket_configuration: NexusBucketConfiguration, step: Step):

        run_from_position = step.value - 1

        if run_from_position == 0:
            return ModelRegistrationPipeline.run(included_models, bucket_configuration)

        if run_from_position == 1:
            return ModelRegistrationPipeline.run(included_models, bucket_configuration,
                                                 save_locally=False)

        if run_from_position > 1:
            return ModelRegistrationPipeline.run(included_models, bucket_configuration,
                                                 save_locally=False,
                                                 start_position=run_from_position,
                                                 register_model=False)

    @staticmethod
    def run(included_models: List[Tuple[str, ModelDescription]],
            bucket_configuration: NexusBucketConfiguration,
            save_locally=True, register_model=True, start_position=2):

        for model_rev, model_description in included_models:

            if save_locally:
                logger.info(">  Loading model data")
                data = ModelDataImpl(SRC_DATA_DIR, DST_DATA_DIR)

                ModelRegistrationPipeline.steps[0].run(
                    model_description=model_description,
                    model_data=data,
                    model_revision=None
                )

            if register_model:
                model = ModelRegistrationPipeline.steps[1].run(
                    model_description=model_description,
                    bucket_configuration=bucket_configuration,
                    model_revision=None
                )
            else:
                model = None

            forge = allocate_forge_session_env(bucket_configuration)

            for step in ModelRegistrationPipeline.steps[start_position:]:
                step.run(
                    model_description=model_description,
                    model_revision=model_rev,
                    model=model,
                    forge=forge,
                    bucket_configuration=bucket_configuration
                )
