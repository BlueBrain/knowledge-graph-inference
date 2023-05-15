from abc import ABC
from typing import Dict, List, Tuple

import json
from os.path import join

from bluegraph import PandasPGFrame
from bluegraph.downstream.utils import transform_to_2d, plot_2d
from bluegraph.backends.gensim import GensimNodeEmbedder
from bluegraph.downstream import EmbeddingPipeline
from bluegraph.downstream.similarity import (ScikitLearnSimilarityIndex, SimilarityProcessor)

from inference_tools.bucket_configuration import NexusBucketConfiguration
from similarity_model.building.model import Model
from similarity_model.building.model_data import ModelData
from similarity_model.building.model_description import ModelDescription


class BrModelData(ModelData):
    def __init__(self, src_data_dir, dst_data_dir):
        super().__init__()

        self.src_data_dir = src_data_dir
        self.dst_data_dir = dst_data_dir


class BBPBrainRegionModelAlone(Model, ABC):
    brain_region_hierarchy: Dict

    def __init__(self, model_data: BrModelData, bucket_configuration: NexusBucketConfiguration):
        self.src_data_dir = model_data.src_data_dir
        self.bucket_configuration = bucket_configuration
        self.brain_region_hierarchy = self.get_bbp_brain_ontology()
        self.visualize = False

    def get_bbp_brain_ontology(self):
        with open(join(self.src_data_dir, "brainregion.json"), "r") as f:
            allen_hierarchy = json.load(f)
            return allen_hierarchy

    def run(self) -> EmbeddingPipeline:
        # Create a property graph from the loaded hierarchy
        list_of_br = self.brain_region_hierarchy["defines"]



        edges: List[Tuple[str, str]] = [
            (br["@id"], br["isPartOf"][0])
            for br in list_of_br
            if "isPartOf" in br
        ]

        nodes: List[str] = list(set([s for el in edges for s in el]))
        allocate_forge_session
        br_res = [forge.retrieve(e) for e in nodes]
        frame = PandasPGFrame()
        frame.add_nodes(nodes)
        frame.add_edges(edges)

        # Train a Poincare embedding model for the hierarchy
        vector_size = 32
        embedder = GensimNodeEmbedder("poincare", size=vector_size, negative=2, epochs=100)
        embedding = embedder.fit_model(frame)

        # np.savetxt("brain_region_embs.tsv", np.array(embedding["embedding"].tolist()),
        #            delimiter="\t")

        # if self.visualize:
        #     embedding_2d = transform_to_2d(embedding["embedding"].tolist())
        #     plot_2d(frame, vectors=embedding_2d)

        similarity_index = ScikitLearnSimilarityIndex(
            dimension=vector_size, similarity="euclidean",
            initial_vectors=embedding["embedding"].tolist()
        )

        point_ids = embedding.index
        sim_processor = SimilarityProcessor(similarity_index, point_ids=point_ids)

        pipeline = EmbeddingPipeline(
            preprocessor=None,
            embedder=None,
            similarity_processor=sim_processor
        )

        return pipeline


bbp_brain_region_alone_model_description = ModelDescription({
    "name": "Brain Region Embedding - BBP Mouse Brain region ontology",
    "description": "Poincare node embedding of brain regions in BBP Mouse Brain region ontology",
    "filename": "brain_region_poincare_bbp",
    "label": "Brain regions BBP Mouse Brain region ontology - Embedded brain regions",
    "distance": "poincare",
    "model": BBPBrainRegionModelAlone
})
