from typing import Optional

from kgforge.core import Resource, KnowledgeGraphForge

from inference_tools.bucket_configuration import NexusBucketConfiguration
from similarity_model.registration.common import check_forge_model, view_processing
from similarity_model.registration.model_registration_step import ModelRegistrationStep
from similarity_model.mappings.es_mappings import BOOSTING_VIEW_MAPPING
from similarity_model.utils import get_boosting_view_id
from similarity_model.building.model_description import ModelDescription


def create_boosting_view(
        model_description: ModelDescription,
        bucket_configuration: NexusBucketConfiguration,
        model_revision: Optional[str] = None,
        model: Optional[Resource] = None,
        forge: Optional[KnowledgeGraphForge] = None
):
    forge, model = check_forge_model(forge, model, model_description=model_description,
                                     bucket_configuration=bucket_configuration)

    view_id = get_boosting_view_id(model)
    resource_types = ["https://neuroshapes.org/SimilarityBoostingFactor"]
    mapping = BOOSTING_VIEW_MAPPING

    return view_processing(
        mapping=mapping, view_id=view_id, resource_types=resource_types,
        bucket_configuration=bucket_configuration,
        model_revision=model_revision, model=model
    )


registration_step_7 = ModelRegistrationStep(
    function_call=create_boosting_view, position=7,
    log_message="Creating boosting view"
)
