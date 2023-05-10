from abc import ABC
from typing import Dict

import json
from os.path import join
import pandas as pd

from bluegraph import PandasPGFrame
from bluegraph.downstream.utils import transform_to_2d, plot_2d
from bluegraph.backends.gensim import GensimNodeEmbedder
from bluegraph.downstream import EmbeddingPipeline
from bluegraph.downstream.similarity import (ScikitLearnSimilarityIndex, SimilarityProcessor)

from similarity_model.building.model import Model
from similarity_model.building.model_data_impl.model_data_impl import ModelDataImpl
from similarity_model.building.model_description import ModelDescription


class BrainRegionModel(Model, ABC):
    brain_region_hierarchy: Dict

    def run(self) -> EmbeddingPipeline:

        # Create a property graph from the loaded hierarchy

        def _get_children(hierarchy, edges, father=None):
            for child in hierarchy['children']:
                acronym = child["acronym"]
                if father:
                    edges.append((acronym, father))
                _get_children(child, edges, acronym)

        allen_edges = []
        _get_children(self.brain_region_hierarchy, allen_edges)

        nodes = list(set([s for el in allen_edges for s in el]))

        allen_ccfv3_frame = PandasPGFrame()
        allen_ccfv3_frame.add_nodes(nodes)
        allen_ccfv3_frame.add_edges(allen_edges)

        # Train a Poincare embedding model for the hierarchy

        embedder = GensimNodeEmbedder("poincare", size=32, negative=2, epochs=100)
        embedding = embedder.fit_model(allen_ccfv3_frame)

        # np.savetxt("brain_region_embs.tsv", np.array(embedding["embedding"].tolist()),
        #            delimiter="\t")

        brain_region_D = embedding["embedding"].iloc[0].shape[0]

        df = embedding.reset_index()[["@id"]]
        df["label"] = df["@id"]
        # df.to_csv("brain_region_meta.tsv", sep="\t", index=None)

        if self.visualize:
            embedding_2d = transform_to_2d(embedding["embedding"].tolist())
            plot_2d(allen_ccfv3_frame, vectors=embedding_2d)

        brain_region_embedding = self.frame._nodes["brainLocation_brainRegion_id"].apply(
            lambda x: embedding.loc[x]
        )

        brain_region_embedding_frame = PandasPGFrame.from_frames(
            nodes=brain_region_embedding, edges=pd.DataFrame()
        )

        similarity_index = ScikitLearnSimilarityIndex(
            dimension=brain_region_D, similarity="euclidean",
            initial_vectors=brain_region_embedding["embedding"].tolist())

        point_ids = brain_region_embedding.index
        sim_processor = SimilarityProcessor(similarity_index, point_ids=point_ids)

        pipeline = EmbeddingPipeline(
            preprocessor=None,
            embedder=None,
            similarity_processor=sim_processor
        )

        return pipeline


class AllenBrainRegionModel(BrainRegionModel):
    def __init__(self, model_data: ModelDataImpl):
        self.src_data_dir = model_data.src_data_dir
        self.brain_region_hierarchy = self.get_allen_hierarchy()
        self.frame = model_data.frame
        self.visualize = False

    def get_allen_hierarchy(self):
        with open(join(self.src_data_dir, "1.json"), "r") as f:
            allen_hierarchy = json.load(f)
            return allen_hierarchy["msg"][0]


class BBPBrainRegionModel(BrainRegionModel):
    def __init__(self, model_data: ModelDataImpl):
        self.src_data_dir = model_data.src_data_dir
        self.brain_region_hierarchy = self.get_bbp_brain_ontology()
        self.frame = model_data.frame
        self.visualize = False

    def get_bbp_brain_ontology(self):
        with open(join(self.src_data_dir, "mba_hierarchy_v3l23split.json"), "r") as f:
            allen_hierarchy = json.load(f)
            return allen_hierarchy


bbp_brain_region_model_description = ModelDescription({
    "name": "SEU NeuronMorphology Brain Region Embedding - BBP Mouse Brain region ontology",
    "description": "Poincare node embedding of brain regions in BBP Mouse Brain region ontology",
    "filename": "SEU_morph_brain_region_poincare_bbp",
    "label": "Brain regions BBP Mouse Brain region ontology",
    "distance": "poincare",
    "model": BBPBrainRegionModel
})

allen_brain_region_model_description = ModelDescription({
    "name": "SEU NeuronMorphology Brain Region Embedding",
    "description": "Poincare node embedding of brain regions in Allen CCFv3 of the SEU neuron"
                   " morphology dataset resources Brain regions (CCfv3)",
    "filename": "SEU_morph_brain_region_poincare_allen",
    "label": "Brain regions (CCfv3)",
    "distance": "poincare",
    "model": AllenBrainRegionModel
})
