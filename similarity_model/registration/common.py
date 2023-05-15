from typing import Tuple, Optional, Dict, List

from inference_tools.bucket_configuration import NexusBucketConfiguration
from similarity_model.building.model_description import ModelDescription
from similarity_model.registration.helper_functions.model import fetch_model
from similarity_model.registration.helper_functions.view import update_es_view_resource_tag, \
    create_es_view_legacy_params, get_es_view, DeltaException
from similarity_model.registration.logger import logger

from kgforge.core import KnowledgeGraphForge, Resource

from similarity_model.utils import get_model_tag


# At the beginning of most registration steps. (C-I)
# Forge and model are not provided if ever these steps
# Are ran alone -> Maybe this should be done in the run alone ModelRegistrationStep?
def check_forge_model(forge: Optional[KnowledgeGraphForge], model: Optional[Resource],
                      model_description: ModelDescription,
                      bucket_configuration: NexusBucketConfiguration) -> \
        Tuple[KnowledgeGraphForge, Resource]:

    if forge is None:
        forge = bucket_configuration.allocate_forge_session()

    if model is None:
        logger.info("1. Fetching model")
        model = fetch_model(forge, model_description)
        if not model:
            raise Exception(f"Error retrieving model {model_description.name}")
    else:
        logger.info("1. Model provided")

    return forge, model


def view_processing(
        mapping: Dict,
        resource_types: List,
        view_id: str,
        bucket_configuration: NexusBucketConfiguration,
        model_revision: Optional[str] = None,
        model: Optional[Resource] = None
) -> Dict:
    model_id = model.id

    resource_tag = get_model_tag(model_id, model_revision)

    try:
        existing_view = get_es_view(
            es_view_id=view_id,
            token=bucket_configuration.get_token(),
            bucket_configuration=bucket_configuration
        )
    except DeltaException as e:
        if e.status_code == 404:
            existing_view = None
        else:
            raise e

    if existing_view is not None:
        logger.info("2. View exists, updating resource tag only")
        rev = existing_view["_rev"]

        updated_view = update_es_view_resource_tag(
            bucket_configuration=bucket_configuration, resource_tag=resource_tag,
            es_view_id=view_id, rev=rev,
            token=bucket_configuration.get_token(),
            view_body=existing_view
        )
        return updated_view

    else:
        logger.info("2. View does not exist, registering the view")

        created_view = create_es_view_legacy_params(
            bucket_configuration=bucket_configuration,
            es_view_id=view_id,
            resource_types=resource_types,
            mapping=mapping,
            token=bucket_configuration.get_token(),
            resource_schemas=None,
            resource_tag=resource_tag,
        )
        return created_view
        # TODO status code check and Exception
