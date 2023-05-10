import numpy as np
import pandas as pd
from bluegraph import PandasPGFrame
from bluegraph.preprocess import ScikitLearnPGEncoder
from bluegraph.downstream.utils import transform_to_2d, plot_2d
from bluegraph.downstream import EmbeddingPipeline
from bluegraph.downstream.similarity import (FaissSimilarityIndex, SimilarityProcessor)

from similarity_model.building.model import Model
from similarity_model.building.model_data_impl.model_data_impl import ModelDataImpl, get_frame_nodes
from similarity_model.building.model_description import ModelDescription


class NeuriteModel(Model):

    def __init__(self, model_data: ModelDataImpl):
        self.neurite_features_df = model_data.neurite_features_df
        self.frame = model_data.frame
        self.visualize = False

    def run(self) -> EmbeddingPipeline:

        neurite_features = pd.concat([
            self.frame._nodes.reset_index()["@id"],
            self.neurite_features_df
        ], axis=1).set_index("@id")

        neurite_frame = PandasPGFrame.from_frames(nodes=neurite_features, edges=pd.DataFrame())

        for c in get_frame_nodes(neurite_frame).columns:
            try:
                neurite_frame.node_prop_as_numeric(c)
            except Exception:
                neurite_frame.node_prop_as_category(c)

        encoder = ScikitLearnPGEncoder(
            node_properties=neurite_frame.node_properties(),
            missing_numeric="impute",
            imputation_strategy="mean")

        encoded_frame = encoder.fit_transform(neurite_frame)

        neurite_features = get_frame_nodes(encoded_frame).rename(
            columns={"features": "neurite_features"})

        data = np.array(neurite_features["neurite_features"].tolist())
        neurite_features["neurite_features"] = (data / data.max()).tolist()

        neurite_dim = len(neurite_features["neurite_features"].iloc[0])

        if self.visualize:
            embedding_2d = transform_to_2d(get_frame_nodes(encoded_frame)["features"].tolist())
            plot_2d(self.frame, vectors=embedding_2d, label_prop="brainLocation_brainRegion_id")

        similarity_index = FaissSimilarityIndex(
            dimension=neurite_dim, similarity="euclidean", n_segments=3
        )

        sim_processor = SimilarityProcessor(similarity_index, point_ids=None)

        point_ids = neurite_features.index
        sim_processor.add(neurite_features["neurite_features"].tolist(), point_ids)

        pipeline = EmbeddingPipeline(
            preprocessor=encoder,
            embedder=None,
            similarity_processor=sim_processor
        )

        return pipeline


neurite_model_description = ModelDescription({
    "name": "SEU NeuronMorphology Neurite Features",
    "description": "Feature encoding model built for neurite features from the SEU neuron "
                   "morphology dataset resources Neurite features",
    "filename": "SEU_morph_neurite_features_euclidean",
    "label": "Neurite features",
    "distance": "euclidean",
    "model": NeuriteModel
})
