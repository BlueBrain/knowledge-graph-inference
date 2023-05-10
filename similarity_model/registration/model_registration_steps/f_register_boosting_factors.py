from typing import Optional

from kgforge.core import Resource, KnowledgeGraphForge

from inference_tools.bucket_configuration import NexusBucketConfiguration
from inference_tools.datatypes.similarity.statistic import Statistic
from inference_tools.similarity.formula import Formula
from similarity_model.registration.model_registration_step import ModelRegistrationStep
from similarity_model.registration.helper_functions.boosting_factor import (
    compute_boosting_factors,
    register_boosting_factors
)
from similarity_model.registration.helper_functions.stat import compute_statistics, Statistics
from similarity_model.registration.logger import logger
from similarity_model.utils import get_similarity_view_id, get_model_tag
from similarity_model.building.model_description import ModelDescription
from similarity_model.registration.common import check_forge_model


def register_boosting_data(
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

    logger.info("2. Retrieving non-boosted statistics")

    non_boosted_stats_resource = forge.search({
        "type": "ElasticSearchViewStatistics",
        "derivation": {"entity": {"id": view_id}},
        "boosted": False
    })

    if non_boosted_stats_resource is not None and len(non_boosted_stats_resource) > 0:
        stat_json = forge.as_json(non_boosted_stats_resource)[0]
        stat_data_object = Statistic(stat_json)
        stats = Statistics(max=stat_data_object.max, min=stat_data_object.min,
                           mean=stat_data_object.mean, std=stat_data_object.std,
                           N=stat_data_object.N)
    else:
        logger.info(">  Could not retrieve non-boosted statistics, computing them...")
        stats = compute_statistics(forge, view_id, score_formula, boosting=None)

    logger.info("3. Computing boosting factors")

    boosting = compute_boosting_factors(forge, view_id, stats, score_formula, neighborhood_size=10)

    resource_tag = get_model_tag(model.id, model_revision)

    logger.info("4. Registering boosting factors")
    register_boosting_factors(forge, view_id=view_id, boosting_factors=boosting,
                              formula=score_formula, tag=resource_tag)


registration_step_6 = ModelRegistrationStep(
    function_call=register_boosting_data, position=6,
    log_message="Registering boosting data"
)
