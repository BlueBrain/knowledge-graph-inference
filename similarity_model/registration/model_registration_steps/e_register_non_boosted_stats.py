from typing import Optional

from kgforge.core import Resource, KnowledgeGraphForge

from inference_tools.bucket_configuration import NexusBucketConfiguration
from inference_tools.similarity.formula import Formula
from similarity_model.registration.model_registration_step import ModelRegistrationStep
from similarity_model.registration.helper_functions.stat import compute_statistics, \
    register_stats
from similarity_model.registration.logger import logger
from similarity_model.utils import get_similarity_view_id, get_model_tag
from similarity_model.registration.common import check_forge_model
from similarity_model.building.model_description import ModelDescription


def register_non_boosted_statistics(
        model_description: ModelDescription,
        bucket_configuration: NexusBucketConfiguration,
        model_revision: Optional[str] = None,
        model: Optional[Resource] = None,
        forge: Optional[KnowledgeGraphForge] = None
):
    forge, model = check_forge_model(forge, model, model_description=model_description,
                                     bucket_configuration=bucket_configuration)

    view_id = get_similarity_view_id(model)
    score_formula = Formula(model.similarity)

    resource_tag = get_model_tag(model.id, model_revision)

    logger.info("2. Computing non-boosted statistics")
    stats = compute_statistics(forge, view_id, score_formula, boosting=None)

    logger.info("3. Registering statistics")

    # register_stats(forge, view_id, stats, formula=score_formula, tag=resource_tag, boosted=False)


registration_step_5 = ModelRegistrationStep(
    function_call=register_non_boosted_statistics, position=5,
    log_message="Registering non-boosted statistics"
)
