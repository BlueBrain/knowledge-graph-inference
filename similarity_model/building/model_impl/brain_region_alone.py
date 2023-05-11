from abc import ABC
from typing import Dict

import json
from os.path import join

from bluegraph import PandasPGFrame
from bluegraph.downstream.utils import transform_to_2d, plot_2d
from bluegraph.backends.gensim import GensimNodeEmbedder
from bluegraph.downstream import EmbeddingPipeline
from bluegraph.downstream.similarity import (ScikitLearnSimilarityIndex, SimilarityProcessor)

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

    def __init__(self, model_data: BrModelData):
        self.src_data_dir = model_data.src_data_dir
        self.brain_region_hierarchy = self.get_bbp_brain_ontology()
        self.visualize = False

    def get_bbp_brain_ontology(self):
        with open(join(self.src_data_dir, "mba_hierarchy_v3l23split.json"), "r") as f:
            allen_hierarchy = json.load(f)
            return allen_hierarchy

    def run(self) -> EmbeddingPipeline:

        # Create a property graph from the loaded hierarchy
        def _get_children(hierarchy, edges, father=None):
            for child in hierarchy['children']:
                br_id = child["id"]
                if father:
                    edges.append((br_id, father))
                _get_children(child, edges, br_id)

        edges = []
        _get_children(self.brain_region_hierarchy, edges)

        def to_id(id_str):
            return f"http://api.brain-map.org/api/v2/data/Structure/{id_str}"

        edges = [(to_id(a), to_id(b)) for a, b in edges]
        nodes = list(set([s for el in edges for s in el]))

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
            initial_vectors=embedding["embedding"].tolist())

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
