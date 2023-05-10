from bluegraph import PandasPGFrame
from bluegraph.preprocess import ScikitLearnPGEncoder
from bluegraph.downstream.utils import transform_to_2d, plot_2d
from bluegraph.preprocess import CooccurrenceGenerator
from bluegraph.backends.stellargraph import StellarGraphNodeEmbedder
from bluegraph.downstream import EmbeddingPipeline
from bluegraph.downstream.similarity import (FaissSimilarityIndex, SimilarityProcessor)

from similarity_model.building.model import Model
from similarity_model.building.model_data_impl.model_data_impl import ModelDataImpl, get_frame_nodes
from similarity_model.building.model_description import ModelDescription


class DendriteModel(Model):

    def __init__(self, model_data: ModelDataImpl):
        self.morphologies_df = model_data.morphologies_df
        self.localization_features = model_data.localisation_features
        self.frame = model_data.frame
        self.visualize = False

    def run(self) -> EmbeddingPipeline:

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

        # Generate the dendrite co-projection graph

        gen = CooccurrenceGenerator(frame)

        dendrite_edges = gen.generate_from_nodes("BasalDendrite_Leaf_Regions",
                                                 compute_statistics=["frequency"])

        # print(dendrite_edges.shape)

        dendrite_co_projection_frame = PandasPGFrame.from_frames(
            nodes=get_frame_nodes(encoded_frame), edges=dendrite_edges
        )

        dendrite_co_projection_frame.edge_prop_as_numeric("frequency")

        # print(len(dendrite_co_projection_frame.isolated_nodes()))

        # Perform dendrite co-occurrence graph embedding

        dendrite_D = 100

        dendrite_embedder = StellarGraphNodeEmbedder(
            "node2vec", length=5, number_of_walks=20,
            epochs=5, embedding_dimension=dendrite_D, edge_weight="frequency",
            random_walk_p=2, random_walk_q=0.2)
        dendrite_embedding = dendrite_embedder.fit_model(dendrite_co_projection_frame)

        dendrite_co_projection_frame.add_node_properties(
            dendrite_embedding.rename(columns={"embedding": "node2vec"})
        )

        if self.visualize:
            embedding_2d = transform_to_2d(
                get_frame_nodes(dendrite_co_projection_frame)["node2vec"].tolist()
            )
            plot_2d(frame, vectors=embedding_2d, label_prop="brainLocation_brainRegion_id")

        # Create and save the pipeline

        similarity_index = FaissSimilarityIndex(
            dimension=dendrite_D, similarity="cosine", n_segments=3)

        sim_processor = SimilarityProcessor(similarity_index, point_ids=None)

        point_ids = dendrite_embedding.index

        sim_processor.add(dendrite_embedding["embedding"].tolist(), point_ids)

        pipeline = EmbeddingPipeline(
            embedder=dendrite_embedder,
            similarity_processor=sim_processor)

        return pipeline


dendrite_model_description = ModelDescription({
    "name": "SEU NeuronMorphology Dendrite Co-Projection Embedding",
    "description": "Node embedding model (node2vec) built on a dendrite co-projection graph "
                   "extracted from the SEU neuron morphology dataset resources Dendrite "
                   "projection",
    "filename": "SEU_morph_dendrite_coproj_node2vec_cosine",
    "label": "Dendrite projection",
    "distance": "cosine",
    "model": DendriteModel
})