import sys
import jwt
from typing import Any, Optional

from kgforge.core import KnowledgeGraphForge, Resource
from kgforge.specializations.resources import Dataset

from similarity_model.building.model_description import ModelDescription

from bluegraph.core.embed.embedders import GraphElementEmbedder
from bluegraph.downstream import EmbeddingPipeline

from similarity_model.registration.logger import logger


def create_model(forge: KnowledgeGraphForge, name: str, description: str,
                 pref_label: str,
                 pipeline_path,
                 distance_metric,
                 vector_dimension=None,
                 bluegraph_version=None) -> Resource:

    """
    Create a new embedding model resource
    @param forge:
    @type forge: KnowledgeGraphForge
    @param name:
    @type name: str
    @param description:
    @type description: str
    @param pref_label:
    @type pref_label: str
    @param pipeline_path:
    @type pipeline_path:
    @param distance_metric:
    @type distance_metric:
    @param vector_dimension:
    @type vector_dimension:
    @param bluegraph_version:
    @type bluegraph_version:
    @return: the id of the new model
    @rtype: str
    """

    model_resource = Dataset(forge, name=name, description=description)
    model_resource.type = "EmbeddingModel"
    model_resource.prefLabel = pref_label
    model_resource.similarity = distance_metric

    if vector_dimension is not None:
        model_resource.vectorDimension = vector_dimension

    # Add distribution
    if pipeline_path is not None:
        model_resource.add_distribution(pipeline_path, content_type="application/octet-stream")

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
    return model_resource


def update_model(forge: KnowledgeGraphForge, model_resource: Resource,
                 new_pipeline_path: Any, vector_dimension=None) -> Resource:
    # TODO types of distrib and vector_dim?

    """Update embedding model file."""
    if vector_dimension is not None:
        model_resource.vectorDimension = vector_dimension

    model_resource.distribution = forge.attach(new_pipeline_path,
                                               content_type="application/octet-stream")
    forge.update(model_resource)
    return model_resource


def fetch_model(forge: KnowledgeGraphForge, model_description: ModelDescription) \
        -> Optional[Resource]:

    result = forge.search({"name": model_description.name, "type": "EmbeddingModel",
                           "_deprecated": False})

    if result is None:
        raise Exception("Could not query for existing models ")
    if len(result) == 0:
        print(f"Model {model_description.name} does not exist")
        return None
    if len(result) != 1:
        print("More than one model persisted with the same name, returning first one")

    return result[0]


def push_model(forge: KnowledgeGraphForge, model_description: ModelDescription,
               description: str, label: str, pipeline_path,
               distance_metric, bluegraph_version=None) -> Resource:
    """
    Push (register or update) an embedding model
    @param forge:
    @type forge: KnowledgeGraphForge
    @param model_description:
    @type model_description: ModelDescription
    @param description:
    @type description: str
    @param label:
    @type label: str
    @param pipeline_path:
    @type pipeline_path:
    @param distance_metric:
    @type distance_metric:
    @param bluegraph_version:
    @type bluegraph_version:
    @return: the id of the updated/created model
    @rtype: str
    """

    existing_model = fetch_model(forge, model_description)

    pipeline = EmbeddingPipeline.load(
        path=pipeline_path, embedder_interface=GraphElementEmbedder, embedder_ext="zip"
    )

    vector_dimension = pipeline.generate_embedding_table().iloc[0]["embedding"].shape[0]

    if existing_model:
        logger.info(">  Model exists, updating...")
        return update_model(forge, existing_model, pipeline_path, vector_dimension=vector_dimension)
        # TODO should only update the distribution or also more?

    logger.info(">  Registering new model...")
    return create_model(
        forge, model_description.name, description, label, pipeline_path,
        distance_metric, vector_dimension=vector_dimension,  bluegraph_version=bluegraph_version)


def get_agent(forge) -> Resource:
    """Create a Nexus agent."""
    token = forge._store.token
    agent_data = jwt.decode(token, options={"verify_signature": False})
    agent = forge.reshape(
        forge.from_json(agent_data),
        keep=["name", "email", "sub", "preferred_username"]
    )
    agent.id = agent.sub
    agent.type = "Person"
    return agent
