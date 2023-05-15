from typing import Tuple, List, Optional

from inference_tools.bucket_configuration import NexusBucketConfiguration
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
from similarity_model.registration.step import Step


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
    def run_many_from(models_information: List[Tuple[str, ModelDescription]],
                      bucket_configuration: NexusBucketConfiguration, step: Step):

        run_from_position = step.value - 1

        if run_from_position == 0:
            return ModelRegistrationPipeline.run_many(models_information, bucket_configuration)

        if run_from_position == 1:
            return ModelRegistrationPipeline.run_many(models_information, bucket_configuration,
                                                      save_locally=False)

        if run_from_position > 1:
            return ModelRegistrationPipeline.run_many(models_information, bucket_configuration,
                                                      save_locally=False,
                                                      start_position=run_from_position,
                                                      register_model=False)

    @staticmethod
    def run(model_information: Tuple[str, ModelDescription],
            bucket_configuration: NexusBucketConfiguration,
            save_locally=True, register_model=True, start_position=2, data: ModelData = None):

        if save_locally:
            data = data if data is not None else ModelRegistrationPipeline.get_default_model_data()

            ModelRegistrationPipeline.steps[0].run(
                model_information=model_information,
                model_data=data,
            )

        if register_model:
            model = ModelRegistrationPipeline.steps[1].run(
                model_information=model_information,
                bucket_configuration=bucket_configuration,
            )
        else:
            model = None

        forge = bucket_configuration.allocate_forge_session()

        for step in ModelRegistrationPipeline.steps[start_position:]:
            step.run(
                model_information=model_information,
                model=model,
                forge=forge,
                bucket_configuration=bucket_configuration
            )

    @staticmethod
    def run_many(models_information: List[Tuple[str, ModelDescription]],
                 bucket_configuration: NexusBucketConfiguration,
                 save_locally=True, register_model=True, start_position=2,
                 data: Optional[ModelData] = None):

        data = data if data is not None else ModelRegistrationPipeline.get_default_model_data()

        for model_information in models_information:
            ModelRegistrationPipeline.run(
                model_information=model_information,
                bucket_configuration=bucket_configuration,
                save_locally=save_locally,
                register_model=register_model,
                start_position=start_position,
                data=data
            )

    @staticmethod
    def get_default_model_data() -> ModelData:
        logger.info(">  Loading model data")
        data = ModelDataImpl(SRC_DATA_DIR, DST_DATA_DIR)
        return data
