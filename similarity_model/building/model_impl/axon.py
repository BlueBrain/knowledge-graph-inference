from bluegraph import PandasPGFrame
from bluegraph.preprocess import ScikitLearnPGEncoder
from bluegraph.preprocess import CooccurrenceGenerator

from bluegraph.downstream.utils import transform_to_2d, plot_2d
from bluegraph.backends.stellargraph import StellarGraphNodeEmbedder
from bluegraph.downstream import EmbeddingPipeline
from bluegraph.downstream.similarity import (FaissSimilarityIndex, SimilarityProcessor)

from similarity_model.building.model import Model
from similarity_model.building.model_data_impl.model_data_impl import get_frame_nodes, ModelDataImpl
from similarity_model.building.model_description import ModelDescription


class AxonModel(Model):

    def __init__(self, model_data: ModelDataImpl):
        self.morphologies_df = model_data.morphologies_df
        self.localization_features = model_data.localisation_features
        self.frame = model_data.frame
        self.visualize = False

    def get_co_projection_frame(self):

        #  1. Create axon/dendrite co-projection property graphs
        frame = self.frame

        props = set(frame.node_properties()).difference({
            "Axon_Section_Regions",
            "Axon_Leaf_Regions",
            "BasalDendrite_Section_Regions",
            "BasalDendrite_Leaf_Regions"})

        encoder = ScikitLearnPGEncoder(
            node_properties=props,
            missing_numeric="impute",
            imputation_strategy="mean",
            reduce_node_dims=True,
            n_node_components=40
        )

        encoded_frame = encoder.fit_transform(frame)

        # print(sum(encoder.node_reducer.explained_variance_ratio_))

        # Generate the axon co-projection graph

        gen = CooccurrenceGenerator(frame)

        axon_edges = gen.generate_from_nodes("Axon_Leaf_Regions",
                                             compute_statistics=["frequency"])

        axon_edges = axon_edges[axon_edges["frequency"].values > 10]

        # print(axon_edges.shape)

        axon_co_projection_frame = PandasPGFrame.from_frames(
            nodes=get_frame_nodes(encoded_frame), edges=axon_edges
        )

        axon_co_projection_frame.edge_prop_as_numeric("frequency")
        return axon_co_projection_frame

    def run(self) -> EmbeddingPipeline:

        axon_co_projection_frame = self.get_co_projection_frame()
        frame = self.frame
        # print(len(axon_co_projection_frame.isolated_nodes()))

        # Perform axon co-projection graph embedding

        axon_D = 128

        axon_embedder = StellarGraphNodeEmbedder(
            "node2vec", length=5, number_of_walks=20,
            epochs=5, embedding_dimension=axon_D, edge_weight="frequency",
            random_walk_p=2, random_walk_q=0.2
        )

        axon_embedding = axon_embedder.fit_model(axon_co_projection_frame)

        axon_co_projection_frame.add_node_properties(
            axon_embedding.rename(columns={"embedding": "node2vec"}))

        if self.visualize:
            embedding_2d = transform_to_2d(
                get_frame_nodes(axon_co_projection_frame)["node2vec"].tolist()
            )
            plot_2d(frame, vectors=embedding_2d, label_prop="brainLocation_brainRegion_id")

        # Create and save the pipeline

        similarity_index = FaissSimilarityIndex(dimension=axon_D, similarity="cosine", n_segments=3)

        sim_processor = SimilarityProcessor(similarity_index, point_ids=None)

        point_ids = axon_embedding.index
        sim_processor.add(axon_embedding["embedding"].tolist(), point_ids)

        pipeline = EmbeddingPipeline(
            embedder=axon_embedder,
            similarity_processor=sim_processor
        )

        return pipeline


axon_model_description = ModelDescription({
    "name": "SEU NeuronMorphology Axon Co-Projection Embedding",
    "description": "Node embedding model (node2vec) built on an axon co-projection graph "
                   "extracted from the SEU neuron morphology dataset resources Axon projection",
    "filename": "SEU_morph_axon_coproj_node2vec_cosine",
    "label": "Axon projection",
    "distance": "cosine",
    "model": AxonModel
})
