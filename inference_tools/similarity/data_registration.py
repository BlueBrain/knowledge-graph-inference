"""."""

import uuid
from .utils import FORMULAS


def vectors_to_resources(forge, vectors, resources, model_id):
    jsons = []
    for i, v in enumerate(vectors):
        json_repr = {
            "@id": str(uuid.uuid4()),
            "@type": ["Entity", "Embedding"],
            "derivation": {
                "@type": "Derivation",
                "entity": {
                  "@id": resources[i],
                  "@type": "Entity"
                }
            },
            "embedding": v.tolist(),
            "generation": {
                "@type": "Generation",
                "activity": {
                  "@type": [
                    "Activity",
                    "EmbeddingActivity"
                  ],
                  "used": {
                    "@id": model_id,
                    "@type": "EmbeddingModel",
                  }
                }
            },
            "name": f"Embedding of {resources[i]}"
        }
        jsons.append(json_repr)
    return forge.from_json(jsons)


def register_stats(forge, view_id, sample_size, stats, formula,
                   tag, boosted=False):
    stat_values = [
        {
          "statistic": "min",
          "unitCode": "dimensionless",
          "value": stats.min
        },
        {
          "statistic": "max",
          "unitCode": "dimensionless",
          "value": stats.max
        },
        {
          "statistic": "mean",
          "unitCode": "dimensionless",
          "value": stats.mean
        },
        {
          "statistic": "standard deviation",
          "unitCode": "dimensionless",
          "value": stats.std
        },
        {
          "statistic": "N",
          "unitCode": "dimensionless",
          "value": sample_size
        }
    ]

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
        stats_resource = stats[0]
        stats_resource.series = forge.from_json(stat_values)
        forge.update(stats_resource)
    else:
        json_data = {
            "type": "ElasticSearchViewStatistics",
            "boosted": boosted,
            "scriptScore": formula,
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


def register_boosting_data(forge, view_id, boosting_factors,
                           formula, tag):
    generation_resource = forge.from_json({
        "type": "Generation",
        "activity": {
            "type": "Activity",
            "used": {
                "id": view_id,
                "type": "AggregateElasticSearchView"
            }
        }
    })
    resources = []
    for k, v in boosting_factors.items():
        existing_data = forge.search({
            "type": "SimilarityBoostingFactor",
            "derivation": {
                "entity": {
                    "id": k
                }
            }
        })
        if len(existing_data) > 0:
            boosting_resource = existing_data[0]
            boosting_resource.value = v
            boosting_resource.generation = generation_resource
            forge.update(boosting_resource)
            resources = existing_data
        else:
            json_data = {
                "type": "SimilarityBoostingFactor",
                "value": v,
                "unitCode": "dimensionless",
                "scriptScore": FORMULAS[formula],
                "vectorParameter": "query_vector",
                "derivation": {
                    "type": "Derivation",
                    "entity": {
                        "id": k,
                        "type": "Embedding"
                    }
                },
            }
            boosting_resource = forge.from_json(json_data)
            boosting_resource.generation = generation_resource
            resources.append(boosting_resource)
            forge.register(boosting_resource)
        forge.tag(boosting_resource, tag)
    return resources
