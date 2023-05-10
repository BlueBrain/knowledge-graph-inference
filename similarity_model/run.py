from typing import Tuple, List

from similarity_model.constants import SRC_DATA_DIR, DST_DATA_DIR
from similarity_model.building.model_data_impl.model_data_impl import ModelDataImpl

from inference_tools.bucket_configuration import NexusBucketConfiguration
from similarity_model.registration.model_registration_pipeline import ModelRegistrationPipeline, \
    Step
from similarity_model.building.model_description import ModelDescription
from similarity_model.registration.logger import logger

from similarity_model.building.model_impl.axon import axon_model_description
from similarity_model.building.model_impl.brain_region import allen_brain_region_model_description
from similarity_model.building.model_impl.brain_region import bbp_brain_region_model_description
from similarity_model.building.model_impl.coordinate import coordinate_model_description
from similarity_model.building.model_impl.dendrite import dendrite_model_description
from similarity_model.building.model_impl.neurite import neurite_model_description
from similarity_model.building.model_impl.tmd import unscaled_model_description
from similarity_model.building.model_impl.tmd import scaled_model_description

included_models: List[Tuple[str, ModelDescription]] = [
    # ("1", dendrite_model_description),
    # ("1", coordinate_model_description),
    ("1", allen_brain_region_model_description),
    # ("1", neurite_model_description),
    # ("1", axon_model_description),
    # ("1", unscaled_model_description),
    # ("1", scaled_model_description),
    # ("1", bbp_brain_region_model_description),
]

bucket_config = NexusBucketConfiguration("bbp-external", "seu", is_prod=True)

# ModelRegistrationPipeline.run(included_models=included_models,
#                               bucket_configuration=bucket_config)

# ModelRegistrationPipeline.run_from(included_models=included_models,
#                                    bucket_configuration=bucket_config,
#                                    step=Step.REGISTER_NON_BOOSTED_STATS)
#

ModelRegistrationPipeline.get_step(Step.REGISTER_NON_BOOSTED_STATS).run(
    model_description=axon_model_description, model_revision=None,
    bucket_configuration=bucket_config)
