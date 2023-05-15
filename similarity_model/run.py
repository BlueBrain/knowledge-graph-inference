from typing import Tuple, List, Optional

from inference_tools.bucket_configuration import NexusBucketConfiguration
from similarity_model.constants import SRC_DATA_DIR, DST_DATA_DIR
from similarity_model.registration.model_registration_pipeline import ModelRegistrationPipeline, \
    Step

from similarity_model.building.model_impl.brain_region_alone import \
    bbp_brain_region_alone_model_description, BrModelData

# from similarity_model.building.model_impl.axon import axon_model_description
# from similarity_model.building.model_impl.brain_region import allen_brain_region_model_description
# from similarity_model.building.model_impl.brain_region import bbp_brain_region_model_description
# from similarity_model.building.model_impl.coordinate import coordinate_model_description
# from similarity_model.building.model_impl.dendrite import dendrite_model_description
# from similarity_model.building.model_impl.neurite import neurite_model_description
# from similarity_model.building.model_impl.tmd import unscaled_model_description
# from similarity_model.building.model_impl.tmd import scaled_model_description
#
# included_models: List[Tuple[Optional[str], ModelDescription]] = [
#     ("1", dendrite_model_description),
#     ("1", coordinate_model_description),
#     ("1", allen_brain_region_model_description),
#     ("1", neurite_model_description),
#     ("1", axon_model_description),
#     ("1", unscaled_model_description),
#     ("1", scaled_model_description),
#     ("1", bbp_brain_region_model_description),
# ]


# bucket_config = NexusBucketConfiguration("bbp-external", "seu", is_prod=True)

# ModelRegistrationPipeline.run_many(models_information=included_models,
#                                    bucket_configuration=bucket_config)
#
# ModelRegistrationPipeline.run_many_from(models_information=included_models,
#                                         bucket_configuration=bucket_config,
#                                         step=Step.REGISTER_NON_BOOSTED_STATS)
#
# bucket_config2 = NexusBucketConfiguration(
#         organisation="neurosciencegraph",
#         project="datamodels",
#         is_prod=True
#     )
#
# model_data = BrModelData(
#     src_data_dir=SRC_DATA_DIR,
#     dst_data_dir=DST_DATA_DIR,
#     bucket_configuration=bucket_config2
# )
#
# ModelRegistrationPipeline.get_step(Step.SAVE_MODEL).run_many(
#     models_information=[("1", bbp_brain_region_alone_model_description)],
#     bucket_configuration=bucket_config2, model_data=model_data
# )

ModelRegistrationPipeline.get_step(Step.REGISTER_MODEL).run_many(
    models_information=[("1", bbp_brain_region_alone_model_description)],
    bucket_configuration=
    NexusBucketConfiguration(organisation="dke", project="embedding-pipelines", is_prod=True)
)


# from bluegraph.core import GraphElementEmbedder
# from bluegraph.downstream import EmbeddingPipeline
# from similarity_model.registration.helper_functions.embedding import get_embedding_vectors_from_pipeline
# from similarity_model.utils import get_path
# import os
# from similarity_model.constants import SRC_DATA_DIR, DST_DATA_DIR, PIPELINE_SUBDIRECTORY
#
#
# pipeline_path = get_path(
#     os.path.join(SRC_DATA_DIR, PIPELINE_SUBDIRECTORY, "brain_region_poincare_bbp.zip")
# )
#
# missing_list, embedding_dict = get_embedding_vectors_from_pipeline(
#     pipeline=EmbeddingPipeline.load(
#         path=pipeline_path, embedder_interface=GraphElementEmbedder, embedder_ext="zip"
#     )
# )
