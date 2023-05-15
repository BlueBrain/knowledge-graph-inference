from string import Template
from typing import Callable, List, Dict

from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.query import SimilaritySearchQuery
from inference_tools.datatypes.query_configuration import SimilaritySearchQueryConfiguration
from inference_tools.exceptions.malformed_rule import MalformedSimilaritySearchQueryException
from inference_tools.similarity.combine import combine_similarity_models
from inference_tools.similarity.single import query_similar_resources
from inference_tools.similarity.similarity_model_result import SimilarityModelResult


def execute_similarity_query(forge_factory: Callable[[str, str], KnowledgeGraphForge],
                             query: SimilaritySearchQuery, parameter_values: Dict):
    """Execute similarity search query.

    Parameters
    ----------
    forge_factory : func
        Factory that returns a forge session given a bucket
    query : dict
        Json representation of the similarity search query (`SimilarityQuery`)
    parameter_values : dict
        Input parameters used in the similarity query

    Returns
    -------
    neighbors : list of resource ID
        List of similarity search results, each element is a resource ID.
    """
    config: List[SimilaritySearchQueryConfiguration] = query.query_configurations

    if config is None:
        raise MalformedSimilaritySearchQueryException("No similarity search configuration provided")

    k = query.k

    if isinstance(k, str):
        k = int(Template(k).substitute(parameter_values))

    if len(config) == 1:
        config_i = config[0]
        forge = forge_factory(config_i.org, config_i.project)

        _, neighbors = query_similar_resources(
            forge_factory=forge_factory, forge=forge,
            query=query, config=config_i, parameter_values=parameter_values, k=k
        )

        return [
            SimilarityModelResult(
                id=n.entity_id,
                score=score,
                score_breakdown={config_i.embedding_model.id: (score, 1)}
            ).to_json()
            for score, n in neighbors
        ]

    return combine_similarity_models(k=k, forge_factory=forge_factory,
                                     parameter_values=parameter_values, query=query)
