from typing import Optional

from kgforge.core import Resource, KnowledgeGraphForge
from inference_tools.bucket_configuration import NexusBucketConfiguration
from similarity_model.registration.model_registration_step import ModelRegistrationStep
from similarity_model.mappings.es_mappings import get_es_view_mappings
from similarity_model.registration.common import view_processing, check_forge_model
from similarity_model.utils import get_similarity_view_id
from similarity_model.building.model_description import ModelDescription


def create_similarity_view(
        model_description: ModelDescription,
        bucket_configuration: NexusBucketConfiguration,
        model_revision: Optional[str] = None,
        model: Optional[Resource] = None,
        forge: Optional[KnowledgeGraphForge] = None
):
    forge, model = check_forge_model(forge, model, model_description=model_description,
                                     bucket_configuration=bucket_configuration)

    resource_types = ["https://neuroshapes.org/Embedding"]
    mapping = get_es_view_mappings(model.vectorDimension)
    view_id = get_similarity_view_id(model)

    view_processing(
        mapping=mapping, view_id=view_id, resource_types=resource_types,
        bucket_configuration=bucket_configuration,
        model_revision=model_revision, model=model
    )


registration_step_4 = ModelRegistrationStep(
    function_call=create_similarity_view, position=4,
    log_message="Creating similarity view"
)
