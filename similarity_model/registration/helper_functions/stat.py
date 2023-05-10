from collections import namedtuple
from typing import List, Dict, Tuple, Optional
from kgforge.core import Resource, KnowledgeGraphForge
import numpy as np

from inference_tools.datatypes.similarity.neighbor import Neighbor
from inference_tools.similarity.formula import Formula
from inference_tools.similarity.single import get_neighbors
from inference_tools.source.elastic_search import ElasticSearch
from similarity_model.registration.logger import logger

Statistics = namedtuple('Statistics', 'min max mean std N')


def compute_statistics(forge: KnowledgeGraphForge, view_id: str, score_formula: Formula,
                       boosting: Dict[str, float] = None) -> Statistics:
    """Compute similarity score statistics given a view."""
    ElasticSearch.set_elastic_view(forge, view_id)
    all_vectors: List[Resource] = ElasticSearch.get_all_documents(forge)

    scores = []

    for i, vector_resource in enumerate(all_vectors):
        if i % 20 == 0:
            logger.info(f">  Neighbors computed for {i}/{len(all_vectors)} neuron morphologies")

        neighbors: Dict[int, Optional[Neighbor]] = get_neighbors(
            vector=vector_resource.embedding,
            forge=forge,  vector_id=vector_resource.id,
            k=len(all_vectors), score_formula=score_formula,
            get_payload=False
        )

        boosting_value = boosting[vector_resource.id] if boosting else 1
        # if len(neighbors) != len(all_vectors) - 1:
        #     print(f"Length of neighbors is not len(vectors)-1: {len(neighbors)} vector {i}")

        scores += [score * boosting_value for score, _ in neighbors.items()]

    scores = np.array(scores)
    print(len(scores))
    return Statistics(scores.min(), scores.max(), scores.mean(), scores.std(), float(len(scores)))


def register_stats(forge: KnowledgeGraphForge, view_id: str, stats: Statistics,
                   formula: Formula, tag, boosted: bool = False):
    """Create ES view statistic resources."""
    stats_dict = {
        "min": stats.min,
        "max": stats.max,
        "mean": stats.mean,
        "standard deviation": stats.std,
        "N": stats.N
    }

    def stats_obj(key, val):
        return {
            "statistic": key,
            "unitCode": "dimensionless",
            "value": val
        }

    stat_values = [stats_obj(key, val) for key, val in stats_dict.items()]

    # Check if a statistics view for this entity id already exists
    stats = forge.search({
        "type": "ElasticSearchViewStatistics",
        "boosted": boosted,
        "derivation": {
            "entity": {
                "id": view_id
            }
        }
    })

    if len(stats) > 0:
        # Statistics view exists, update it
        stats_resource = stats[0]
        stats_resource.series = forge.from_json(stat_values)
        forge.update(stats_resource)
    else:
        # Statistics view does not exist, create it
        json_data = {
            "type": "ElasticSearchViewStatistics",
            "boosted": boosted,
            "scriptScore": formula.value,
            "series": stat_values,
            "derivation": {
                "type": "Derivation",
                "entity": {
                    "id": view_id
                }
            }
        }

        stats_resource = forge.from_json(json_data)
        forge.register(stats_resource)

    forge.tag(stats_resource, tag)

    return stats_resource
