"""Utils for registering similarity-related resources in Nexus."""

import jwt
import os
import sys
import uuid
import warnings

from collections import namedtuple

from .utils import FORMULAS

from kgforge.core import KnowledgeGraphForge
from kgforge.specializations.resources import Dataset
from kgforge.specializations.mappings import DictionaryMapping

from bluegraph.downstream import EmbeddingPipeline
from bluegraph.core import GraphElementEmbedder


BucketConfiguration = namedtuple(
    'BucketConfiguration', 'endpoint org proj')


def create_forge_session(config_path, bucket_config, token):
    """Create a forge session."""
    endpoint, org, proj = bucket_config
    return KnowledgeGraphForge(
        config_path,
        token=token,
        endpoint=endpoint,
        bucket=f"{org}/{proj}")


def get_agent(forge):
    """Create a Nexus agent."""
    token = forge._store.token
    agent_data = jwt.decode(token, verify=False)
    agent = forge.reshape(
        forge.from_json(agent_data), keep=[
            "name", "email", "sub", "preferred_username"])
    agent.id = agent.sub
    agent.type = "Person"
    return agent


def register_model(forge, name, description, label, distribution,
                   similarity, dimension, bluegraph_version=None):
    """Create a new embedding model resource."""
    model_resource = Dataset(
        forge,
        name=name,
        description=description)
    model_resource.type = "EmbeddingModel"
    model_resource.prefLabel = label
    model_resource.similarity = similarity
    model_resource.vectorDimension = dimension

    # Add distrubution
    if distribution is not None:
        model_resource.add_distribution(
            distribution, content_type="application/octet-stream")

    # Add contribution
    agent = get_agent(forge)
    model_resource.add_contribution(agent, versioned=False)
    role = forge.from_json({
        "hadRole": {
            "id": "http://purl.obolibrary.org/obo/CRO_0000064",
            "label": "software engineering role"
        }
    })
    model_resource.contribution.hadRole = role

    # Add software agent
    software_agent = {
        "type": "SoftwareAgent",
        "description": "Unifying Python framework for graph analytics and co-occurrence analysis.",
        "name": "BlueGraph",
        "softwareSourceCode": {
            "type": "SoftwareSourceCode",
            "codeRepository": "https://github.com/BlueBrain/BlueGraph",
            "programmingLanguage": "Python",
            "runtimePlatform": f"{sys.version_info.major}.{sys.version_info.minor}",
            "version": bluegraph_version
        }
    }
    model_resource.wasAssociatedWith = software_agent
    
    forge.register(model_resource)
    return model_resource.id


def update_model_distribution(forge, model_resource, new_distribution,
                              vector_dim=None):
    """Update embedding model file."""
    if vector_dim is not None:
        model_resource.vectorDimension = vector_dim
    model_resource.distribution = forge.attach(
        new_distribution, content_type="application/octet-stream")
    forge.update(model_resource)
    return model_resource.id
    

def push_model(forge, name, description, label, distribution,
               similarity, dimension, bluegraph_version=None):
    result = forge.search({"name": name})
    """Push (register or update) an embedding model."""
    if result:
        print("Model exists, updating...")
        model_resource = result[0]
        return update_model_distribution(
            forge, model_resource, distribution, dimension)
    else:
        print("Registering new model...")
        return register_model(
            forge, name, description, label, distribution,
            similarity, dimension, bluegraph_version)


def register_embeddings(forge, vectors, model_id, model_revision, tag,mapping_path):
    """Register embedding vectors."""
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
                "embedding_name": f"Embedding of {at_id.split('/')[-1]} at revision TODO" ,
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
    new_embedding_resources = forge.from_json(
        forge.as_json(new_embedding_resources))
    forge.register(new_embedding_resources)
    for r in new_embedding_resources:
        forge.tag(forge.retrieve(r.id), tag)
    print("Updated: ", len(updated_embeddings))
    forge.update(updated_embeddings)
    print("Tagging updated resources...")
    forge.tag(updated_embeddings, tag)


def vectors_to_resources(forge, vectors, resources, model_id):
    """Create resources from vectors."""
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
    """Create ES view statistic resources."""
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
    """Create similarity score boosting factor resources."""
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
    """Add new views with replacement."""
    new_views = {el["_project"]: el for el in new_views}
    existing_views = {el["_project"]: el for el in existing_views}
    existing_views.update(new_views)
    return list(existing_views.values())


def load_embedding_models(forge_models, model_ids,
                          model_revisions=dict, dowload_dir="."):
    """Load embedding models into memory."""
    model_resources = []
    model_revisions = {}
    model_tags = {}
    for model_id in model_ids:
        model_revision = model_revisions.get(model_id)
        model_resource = forge_models.retrieve(
            f"{model_id}{'?rev=' + str(model_revision) if model_revision is not None else ''}")
        model_resources.append(model_resource)

        # If revision is not provided by the user, fetch the latest
        if model_revision is None:
            model_revision = model_resource._store_metadata._rev 
            model_revisions[model_id] = model_revision

        tag = f"{model_id.split('/')[-1]}?rev={model_revision}"
        model_tags[model_id] = tag
        
    pipeline_paths = {}
    for model_resource in model_resources:
        forge_models.download(
            model_resource, "distribution.contentUrl",
            dowload_dir, overwrite=True)
        pipeline_paths[model_resource.id] = os.path.join(
            dowload_dir, model_resource.distribution.name)

    pipelines = {}
    for k, pipeline_path in pipeline_paths.items():
        pipelines[k] = EmbeddingPipeline.load(
            pipeline_path,
            embedder_interface=GraphElementEmbedder,
            embedder_ext="zip")

    return model_revisions, model_tags, pipelines


def push_embedding_vectors(forge_sessions, data_buckets, model_ids,
                           model_revisions, model_tags,
                           pipelines, resource_set, mapping):
    """Push embedding vectors to Nexus."""
    for i, model_id in enumerate(model_ids):
        print(f"Processing model '{model_id}'")
        embedding_table = pipelines[model_id].generate_embedding_table()
        for bucket_config, resources in resource_set.items():
            vectors = {}
            for resource in resources:
                if resource.id not in vectors:
                    if resource.id in embedding_table.index:
                        vectors[resource.id] = embedding_table.loc[
                            resource.id].tolist()[0].tolist()
                    else:
                        warnings.warn(
                            f"\tEmbedding vector for '{resource.id}' in '{bucket_config}' was not computed")
            embedding_bucket = data_buckets[bucket_config]
            forge = forge_sessions[embedding_bucket]
            print(
                f"\tRegistering/updating {len(vectors)} vectors for '{embedding_bucket}...'")
            register_embeddings(
                forge, vectors, model_id, model_revisions[model_id],
                model_tags[model_id], mapping)
