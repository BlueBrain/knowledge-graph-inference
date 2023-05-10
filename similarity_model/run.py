from typing import Tuple, List, Optional

from inference_tools.bucket_configuration import NexusBucketConfiguration
from similarity_model.registration.model_registration_pipeline import ModelRegistrationPipeline, \
    Step
from similarity_model.building.model_description import ModelDescription

from similarity_model.building.model_impl.axon import axon_model_description
from similarity_model.building.model_impl.brain_region import allen_brain_region_model_description
from similarity_model.building.model_impl.brain_region import bbp_brain_region_model_description
from similarity_model.building.model_impl.coordinate import coordinate_model_description
from similarity_model.building.model_impl.dendrite import dendrite_model_description
from similarity_model.building.model_impl.neurite import neurite_model_description
from similarity_model.building.model_impl.tmd import unscaled_model_description
from similarity_model.building.model_impl.tmd import scaled_model_description

included_models: List[Tuple[Optional[str], ModelDescription]] = [
    ("1", dendrite_model_description),
    ("1", coordinate_model_description),
    ("1", allen_brain_region_model_description),
    ("1", neurite_model_description),
    ("1", axon_model_description),
    ("1", unscaled_model_description),
    ("1", scaled_model_description),
    ("1", bbp_brain_region_model_description),
]

bucket_config = NexusBucketConfiguration("bbp-external", "seu", is_prod=True)

# ModelRegistrationPipeline.run_many(models_information=included_models,
#                                    bucket_configuration=bucket_config)
#
# ModelRegistrationPipeline.run_many_from(models_information=included_models,
#                                         bucket_configuration=bucket_config,
#                                         step=Step.REGISTER_NON_BOOSTED_STATS)

ModelRegistrationPipeline.get_step(Step.REGISTER_BOOSTING_FACTORS).run_many(
    models_information=included_models,
    bucket_configuration=bucket_config
)
