"""."""

import uuid
from collections import namedtuple

from .utils import FORMULAS

from kgforge.core import KnowledgeGraphForge
from kgforge.specializations.mappings import DictionaryMapping


BucketConfiguration = namedtuple(
    'BucketConfiguration', 'endpoint org proj')


def create_forge_session(config_path, bucket_config, token):
    return KnowledgeGraphForge(
        config_path,
        token=token,
        endpoint=bucket_config.endpoint,
        bucket=f"{bucket_config.org}/{bucket_config.proj}")


def register_embeddings(forge, vectors, model_id, model_revision, tag, mapping_path):
    new_embeddings = []
    updated_embeddings = []
    for at_id, embedding in vectors.items():
        existing_vectors = forge.search({
            "type": "Embedding",
            "derivation": {
                "entity": {
                    "id": at_id
                }
            },
            "generation": {
                "activity": {
                    "used": {
                        "id": model_id
                    }
                }
            }
        })
        if existing_vectors:
            vector_resource = existing_vectors[0]
            vector_resource.embedding = embedding
            vector_resource.generation.activity.used.hasSelector =\
                forge.from_json({
                    "type": "FragmentSelector",
                    "conformsTo": "https://bluebrainnexus.io/docs/delta/api/resources-api.html#fetch",
                    "value": f"?rev={model_revision}"
                })
            updated_embeddings.append(vector_resource)
        else:
            new_embeddings.append({
                "morphology_id": at_id,
                "morphology_rev": "TODO",
                "model_id": model_id,
                "model_rev": model_revision,
                "embedding_name": f"Embedding of morphology {at_id.split('/')[-1]} at revision TODO" ,
                "embedding": embedding,
                "uuid": at_id.split("/")[-1]

            })
    mapping = DictionaryMapping.load(mapping_path)
    new_embedding_resources = forge.map(new_embeddings, mapping)
    for r in new_embedding_resources:
        r.id = forge.format("identifier", "embeddings", str(uuid.uuid4()))
#    # There is some error with registering new resources, so instead of
#     forge.register(new_embedding_resources)
#     print("Tagging new resources...")
#     forge.tag(new_embedding_resources, tag)
#   # I do the following:
    new_embedding_resources = forge.from_json(forge.as_json(new_embedding_resources))
    forge.register(new_embedding_resources)
    for r in new_embedding_resources:
        forge.tag(forge.retrieve(r.id), tag)

    forge.update(updated_embeddings)
    print("Tagging updated resources...")
    forge.tag(updated_embeddings, tag)


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


def add_views_with_replacement(existing_views, new_views):
    new_views = {el["_project"]: el for el in new_views}
    existing_views = {el["_project"]: el for el in existing_views}
    existing_views.update(new_views)
    return list(existing_views.values())
