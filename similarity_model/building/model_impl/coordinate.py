import numpy as np
import pandas as pd

from bluegraph import PandasPGFrame
from bluegraph.downstream.utils import transform_to_2d, plot_2d
from bluegraph.downstream import EmbeddingPipeline
from bluegraph.downstream.similarity import (FaissSimilarityIndex, ScikitLearnSimilarityIndex,
                                             SimilarityProcessor)

from similarity_model.building.model import Model
from similarity_model.building.model_data_impl.model_data_impl import ModelDataImpl
from similarity_model.building.model_description import ModelDescription

DIMENSION = 3


class CoordinateModel(Model):

    def __init__(self, model_data: ModelDataImpl):
        self.frame = model_data.frame
        self.morphologies_df = model_data.morphologies_df
        self.full_df = model_data.full_df
        self.visualize = False

    def run(self) -> EmbeddingPipeline:

        coordinate_df = pd.DataFrame(self.morphologies_df["id"])

        coordinate_df["coordinates"] = pd.Series(self.full_df[[
            "brainLocation.coordinatesInBrainAtlas.valueX.@value",
            "brainLocation.coordinatesInBrainAtlas.valueY.@value",
            "brainLocation.coordinatesInBrainAtlas.valueZ.@value"
        ]].values.tolist()).apply(lambda x: [float(el) for el in x])

        coordinate_df = coordinate_df.rename(columns={"id": "@id"})

        # Scale coordinates by dividing by the maximum value

        coordinates = np.array(coordinate_df["coordinates"].tolist())
        coordinate_df["coordinates"] = (coordinates / coordinates.max()).tolist()

        coordinate_df = coordinate_df.set_index("@id")

        if self.visualize:
            embedding_2d = transform_to_2d(
                [[float(v) for v in el] for el in coordinate_df["coordinates"].tolist()]
            )
            plot_2d(self.frame, vectors=embedding_2d, label_prop="brainLocation_brainRegion_id")

        coordinates_frame = PandasPGFrame.from_frames(nodes=coordinate_df, edges=pd.DataFrame())

        similarity_index = FaissSimilarityIndex(
            dimension=DIMENSION, similarity="euclidean", n_segments=3
        )

        sim_processor = SimilarityProcessor(similarity_index, point_ids=None)

        point_ids = coordinate_df.index
        sim_processor.add(coordinate_df["coordinates"].tolist(), point_ids)

        pipeline = EmbeddingPipeline(
            preprocessor=None,
            embedder=None,
            similarity_processor=sim_processor
        )

        return pipeline


coordinate_model_description = ModelDescription({
    "name": "SEU NeuronMorphology Coordinates",
    "description": "Coordinate similarity of SEU neuron morphology dataset resources "
                   "Coordinates",
    "filename": "SEU_morph_coordinates_euclidean",
    "label": "Coordinates",
    "distance": "euclidean",
    "model": CoordinateModel
})
