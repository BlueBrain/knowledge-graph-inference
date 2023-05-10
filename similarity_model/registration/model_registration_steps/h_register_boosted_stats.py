from typing import Optional

from kgforge.core import Resource, KnowledgeGraphForge

from inference_tools.bucket_configuration import NexusBucketConfiguration
from inference_tools.similarity.formula import Formula
from inference_tools.source.elastic_search import ElasticSearch
from similarity_model.registration.common import check_forge_model
from similarity_model.registration.model_registration_step import ModelRegistrationStep
from similarity_model.registration.helper_functions.stat import compute_statistics, \
    register_stats
from similarity_model.registration.logger import logger
from similarity_model.registration.step import Step
from similarity_model.utils import get_similarity_view_id, get_model_tag, get_boosting_view_id
from similarity_model.building.model_description import ModelDescription


def register_boosted_statistics(
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

    boosting_id = get_boosting_view_id(model)

    ElasticSearch.set_elastic_view(forge, boosting_id)
    boosting_data = ElasticSearch.get_all_documents(forge)
    boosting_data = dict((b.derivation.entity.id, b.value) for b in boosting_data)
    resource_tag = get_model_tag(model.id, model_revision)

    logger.info("2. Computing boosted statistics")
    stats = compute_statistics(forge, view_id, score_formula, boosting=boosting_data)

    logger.info("3. Registering statistics")

    register_stats(forge, view_id, stats, formula=score_formula, tag=resource_tag, boosted=True)


registration_step_8 = ModelRegistrationStep(
    function_call=register_boosted_statistics,
    step=Step.REGISTER_BOOSTED_STATS,
    log_message="Registering boosted statistics"
)
