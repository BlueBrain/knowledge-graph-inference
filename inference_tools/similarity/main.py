from collections import defaultdict
from string import Template
from typing import Callable, List, Dict, Tuple, Optional

from pandas import DataFrame

from kgforge.core import KnowledgeGraphForge

from inference_tools.exceptions.exceptions import SimilaritySearchException
from inference_tools.datatypes.similarity.neighbor import Neighbor
from inference_tools.nexus_utils.forge_utils import ForgeUtils
from inference_tools.datatypes.query import SimilaritySearchQuery
from inference_tools.datatypes.query_configuration import SimilaritySearchQueryConfiguration
from inference_tools.exceptions.malformed_rule import MalformedSimilaritySearchQueryException
from inference_tools.similarity.queries.get_boosting_factors import get_boosting_factors
from inference_tools.similarity.queries.get_embedding_vector import get_embedding_vector
from inference_tools.similarity.queries.get_neighbors import get_neighbors
from inference_tools.similarity.queries.get_score_stats import get_score_stats
from inference_tools.similarity.similarity_model_result import SimilarityModelResult
from inference_tools.datatypes.parameter_specification import ParameterSpecification


SIMILARITY_MODEL_SELECT_PARAMETER_NAME = "SelectModelsParameter"


def execute_similarity_query(
        forge_factory: Callable[[str, str], KnowledgeGraphForge],
        query: SimilaritySearchQuery, parameter_values: Dict, debug: bool,
        use_forge: bool, limit: int
):
    """Execute similarity search query.

    Parameters
    ----------
    forge_factory : func
        Factory that returns a forge session given a bucket
    query : dict
        Json representation of the similarity search query (`SimilarityQuery`)
    parameter_values : dict
        Input parameters used in the similarity query
    debug: bool
    use_forge: bool
    limit: int

    Returns
    -------
    neighbors : list of resource ID
        List of similarity search results, each element is a resource ID.
    """

    target_parameter = query.search_target_parameter

    if target_parameter is None:
        raise MalformedSimilaritySearchQueryException("Target parameter is not specified")

    config: List[SimilaritySearchQueryConfiguration] = query.query_configurations

    if config is None:
        raise MalformedSimilaritySearchQueryException("No similarity search configuration provided")

    try:
        selected_models_spec: ParameterSpecification = next(
            p for p in query.parameter_specifications
            if p.name == SIMILARITY_MODEL_SELECT_PARAMETER_NAME
        )
        selected_models = selected_models_spec.get_value(parameter_values)
    except StopIteration:
        selected_models = [config_i.embedding_model_data_catalog.id for config_i in config]
        # Keep all if SIMILARITY_MODEL_SELECT_PARAMETER_NAME is not a part of the parameter
        # specification = all models should be kept

    valid_configs = [
        config_i for config_i in config
        if config_i.embedding_model_data_catalog.id in selected_models
    ]

    # selected model = embedding model catalog id

    if len(valid_configs) == 0:
        return []

    if len(valid_configs) == 1:
        config_i = valid_configs[0]
        forge = forge_factory(config_i.org, config_i.project)

        _, neighbors = query_similar_resources(
            forge=forge,
            target_parameter=target_parameter,
            config=config_i,
            parameter_values=parameter_values,
            k=limit,
            result_filter=query.result_filter,
            debug=debug,
            use_forge=use_forge
        )

        return [
            SimilarityModelResult(
                id=n.entity_id,
                score=score,
                score_breakdown={config_i.embedding_model_data_catalog.id: (score, 1)}
            ).to_json()
            for score, n in neighbors
        ]

    return combine_similarity_models(
        k=limit,
        forge_factory=forge_factory,
        parameter_values=parameter_values,
        configurations=valid_configs,
        target_parameter=target_parameter,
        result_filter=query.result_filter,
        debug=debug,
        use_forge=use_forge
    )


def query_similar_resources(
        forge: KnowledgeGraphForge,
        config: SimilaritySearchQueryConfiguration,
        parameter_values, k: Optional[int], target_parameter: str,
        result_filter: Optional[str], debug: bool, use_forge: bool = False
) -> Tuple[str, List[Tuple[int, Neighbor]]]:
    """Query similar resources using the similarity query.

    Parameters
    ----------
    forge : KnowledgeGraphForge
        Instance of a forge session
    config: dict or list of dict
        Query configuration containing references to the target views
        to be queried.
    parameter_values : dict
        Input parameters used in the similarity query
    k : int
        Number of nearest neighbors to query
    target_parameter: str
        The name of the input parameter that holds the id of the entity the results should be
        similar to
    result_filter: Optional[str]
        An additional elastic search query filter to apply onto the neighbor search, in string
        format
    debug: bool
    use_forge: bool

    Returns
    -------
    result :  Tuple[str, Dict[int, Neighbor]]
        The id of the embedding vector of the resource being queried, as well as a dictionary
        with keys being scores and values being a Neighbor object holding
        the resource id that is similar

    """
    # Set ES view from the config
    if config.similarity_view.id is None:
        raise MalformedSimilaritySearchQueryException("Similarity search view is not defined")

    ForgeUtils.set_elastic_search_view(forge, config.similarity_view.id)

    search_target = parameter_values.get(target_parameter, None)  # TODO should it be formatted ?

    if search_target is None:
        raise SimilaritySearchException(f"Target parameter value is not specified, a value for the"
                                        f"parameter {target_parameter} is necessary")

    embedding = get_embedding_vector(
        forge, search_target, debug=debug, use_forge=use_forge,
        derivation_type=config.embedding_model_data_catalog.about
    )

    result: List[Tuple[int, Neighbor]] = get_neighbors(
        forge=forge, vector_id=embedding.id, vector=embedding.vector,
        k=k, score_formula=config.embedding_model_data_catalog.distance,
        result_filter=result_filter, parameters=parameter_values, debug=debug,
        use_forge=use_forge, get_derivation=True,
        derivation_type=config.embedding_model_data_catalog.about
    )

    return embedding.id, result


def combine_similarity_models(
        forge_factory: Callable[[str, str], KnowledgeGraphForge],
        configurations: List[SimilaritySearchQueryConfiguration],
        parameter_values: Dict, k: int, target_parameter: str,
        result_filter: Optional[str], debug: bool, use_forge: bool
) -> List[Dict]:
    """Perform similarity search combining several similarity models"""

    model_ids = [config_i.embedding_model_data_catalog.id for config_i in configurations]

    equal_contribution = 1 / len(configurations)  # TODO change to user input model weight

    weights = dict(
        (model_id, equal_contribution)
        for model_id in model_ids
    )

    # Combine the results
    combined_results = defaultdict(dict)

    for config_i in configurations:
        # Assume boosting factors and stats are in the same bucket as embeddings
        forge_i = forge_factory(config_i.org, config_i.project)

        vector_id, neighbors = query_similar_resources(
            forge=forge_i, config=config_i,
            parameter_values=parameter_values, k=None, target_parameter=target_parameter,
            result_filter=result_filter, debug=debug, use_forge=use_forge
        )

        statistic = get_score_stats(
            forge_i, config_i, boosted=config_i.boosted, use_forge=use_forge
        )

        if config_i.boosted:
            boosted_factors = get_boosting_factors(forge_i, config_i, use_forge=use_forge)
            if vector_id not in boosted_factors:
                print(f"Warning {vector_id} not in boosted_factors")
            # TODO temporary because some boosting factors are missing
            factor = boosted_factors[vector_id].value if vector_id in boosted_factors else 1
        else:
            factor = 1

        for score_i, n in neighbors:
            score_i = normalize(score_i * factor, statistic.min, statistic.max)

            combined_results[n.entity_id][config_i.embedding_model_data_catalog.id] = \
                (score_i, weights[config_i.embedding_model_data_catalog.id])

            # weight is redundant but for confirmation
            # score of proximity between n.entity_id and
            # queried resource for the key model

    def get_weighted_score(score_dict):
        if not all([model_id in score_dict.keys() for model_id in model_ids]):
            # TODO If a score is not available for all models,
            #  most likely due to missing data in Nexus, should not happen
            # TODO change this ???
            return 0

        return sum([score * weight for score, weight in score_dict.values()])

    combined_results_mean = [
        (key, get_weighted_score(value), value)
        for key, value in combined_results.items()
    ]

    df = DataFrame(combined_results_mean).nlargest(k, columns=[1])

    return [
        SimilarityModelResult(
            id=id_, score=score, score_breakdown=score_breakdown
        ).to_json()

        for id_, score, score_breakdown in zip(df[0], df[1], df[2])
    ]


def normalize(score, min_v, max_v):
    return (score - min_v) / (max_v - min_v)
