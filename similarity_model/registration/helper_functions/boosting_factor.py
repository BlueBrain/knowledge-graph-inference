from typing import Dict, List, Tuple, Optional

import math
import json

import numpy as np

from inference_tools.forge_utils.forge_utils import set_elastic_search_view
from inference_tools.similarity.formula import Formula
from inference_tools.source.elastic_search import ElasticSearch

from kgforge.core import Resource, KnowledgeGraphForge

from similarity_model.registration.helper_functions.stat import Statistics
from similarity_model.registration.logger import logger


def _compute_score_deviation(forge, point_id, vector, score_min, score_max, k, formula: Formula):
    """
    Compute similarity score deviation for each vector
    @param forge:
    @type forge:
    @param point_id:
    @type point_id:
    @param vector:
    @type vector:
    @param score_min:
    @type score_min:
    @param score_max:
    @type score_max:
    @param k:
    @type k:
    @param formula:
    @type formula:
    @return:
    @rtype:
    """
    query = {
        "size": k,
        "query": {
            "script_score": {
                "query": {
                    "exists": {
                        "field": "embedding"
                    }
                },
                "script": {
                    "source": formula.get_formula(),
                    "params": {
                        "query_vector": vector
                    }
                }
            }
        }
    }

    def normalize(score, min_v, max_v):
        return (score - min_v) / (max_v - min_v)

    result = forge.elastic(json.dumps(query))
    if result is None:
        raise Exception("Score deviation")

    scores = set(
        normalize(el._store_metadata._score, score_min, score_max)
        for el in result if point_id != el.id
    )

    scores = 1 - np.array(list(scores))

    def spherical_gaussian_standard_deviation(value):
        return math.sqrt((value ** 2).mean())

    return spherical_gaussian_standard_deviation(scores)


def compute_boosting_factors(forge: KnowledgeGraphForge, view_id: str, stats: Statistics,
                             formula: Formula, neighborhood_size: int = 10) -> Dict[str, float]:
    """
    Compute boosting factors for all vectors
    @param forge:
    @type forge:
    @param view_id:
    @type view_id:
    @param stats:
    @type stats:
    @param formula:
    @type formula:
    @param neighborhood_size:
    @type neighborhood_size:
    @return:
    @rtype:
    """
    set_elastic_search_view(forge, view_id)

    def compute_boosting_factor(vector_resource: Resource) -> Tuple[str, float]:
        key: str = vector_resource.id
        # Compute local similarity deviations for points
        value: float = 1 + _compute_score_deviation(
            forge=forge, point_id=key,
            vector=vector_resource.embedding,
            score_min=stats.min, score_max=stats.max,
            k=neighborhood_size, formula=formula
        )

        return key, value

    all_vectors: List[Resource] = ElasticSearch.get_all_documents(forge)
    return dict(compute_boosting_factor(vector_resource) for vector_resource in all_vectors)


def register_boosting_factors(forge: KnowledgeGraphForge, view_id: str,
                              boosting_factors: Dict[str, float],
                              formula: Formula, tag: str):
    """
    Create similarity score boosting factor resources
    @param forge:
    @type forge:
    @param view_id: the boosting view id
    @type view_id: str
    @param boosting_factors: the boosting data to register, a dictionary with entity ids as keys,
    and boosting values as values
    @type boosting_factors: Dict[str, float]
    @param formula:
    @type formula:
    @param tag:
    @type tag:
    @return:
    @rtype:
    """

    generation_json = {
        "type": "Generation",
        "activity": {
            "type": "Activity",
            "used": {
                "id": view_id,
                "type": "ElasticSearchView"
            }
        }
    }
    generation_resource = forge.from_json(generation_json)

    def search_boosting_factor(entity_id: str) -> Optional[List[Resource]]:
        return forge.search({
            "type": "SimilarityBoostingFactor",
            "derivation": {
                "entity": {
                    "id": entity_id
                }
            }
        })

    def update_boosting_factor(existing_boosting_factor: Resource, boosting_value: float)\
            -> Resource:
        existing_boosting_factor.value = boosting_value
        existing_boosting_factor.generation = generation_resource
        return existing_boosting_factor
    
    def register_boosting_factor(entity_id: str, boosting_value: float) -> Resource:
        json_data = {
            "type": "SimilarityBoostingFactor",
            "value": boosting_value,
            "unitCode": "dimensionless",
            "scriptScore": formula.get_formula(),
            "vectorParameter": "query_vector",
            "derivation": {
                "type": "Derivation",
                "entity": {
                    "id": entity_id,
                    "type": "Embedding"
                }
            },
            "generation": generation_json
        }
        boosting_resource = forge.from_json(json_data)
        boosting_resource.generation = generation_resource
        return boosting_resource
    
    new_boosting_factors: List[Resource] = []
    updated_boosting_factors: List[Resource] = []
    
    for e_id, boosting_val in boosting_factors.items():
        # Look for boosting factor associated to this entity id
        existing_data = search_boosting_factor(e_id)
        
        # Boosting instance exists for this entity id, update it
        if len(existing_data) > 0:
            updated_boosting_resource = update_boosting_factor(existing_data[0], boosting_val)
            updated_boosting_factors.append(updated_boosting_resource)
        else:
            # No boosting instance exists for this entity id, create it
            created_boosting_resource = register_boosting_factor(e_id, boosting_val)
            new_boosting_factors.append(created_boosting_resource)

    logger.info(f">  Created boosting factors: {len(new_boosting_factors)}")
    forge.register(new_boosting_factors)

    logger.info(f">  Updated boosting factors: {len(updated_boosting_factors)}")
    forge.update(updated_boosting_factors)

    created_boosting_factors = [forge.retrieve(r.id) for r in new_boosting_factors]

    logger.info(">  Tagging created boosting factors...")
    forge.tag(created_boosting_factors, tag)

    logger.info(">  Tagging updated boosting factors...")
    forge.tag(updated_boosting_factors, tag)
