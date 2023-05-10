import json
from abc import ABC
from typing import Dict, List

import os
import numpy as np

import matplotlib.pyplot as plt
from kgforge.core import Resource
from tmd.Neuron.Neuron import Neuron

from sklearn.decomposition import PCA
from bluegraph.downstream import EmbeddingPipeline
from bluegraph.downstream.similarity import (ScikitLearnSimilarityIndex, SimilarityProcessor)

from similarity_model.building.model import Model

from similarity_model.building.model_data_impl.model_data_impl import (
    ModelDataImpl,
    get_persistence_data,
    to_tmd_neuron,
    load_morphologies
)

from similarity_model.building.model_description import ModelDescription
from similarity_model.registration.logger import logger
from similarity_model.utils import encode_id_rev


class TMDModel(Model, ABC):

    def __init__(self, model_data: ModelDataImpl):

        self.visualize = False
        self.re_compute = True
        self.re_download = False

        diagram_all_neurites, max_time = TMDModel.get_persistence_diagrams(
            data_dir=model_data.src_data_dir,
            re_download=self.re_download,
            re_compute=self.re_compute,
            forge_seu=model_data.forge_seu,  # only if recomputing
            morphologies=model_data.morphologies  # only if recomputing
        )

        self.diagram_all_neurites = diagram_all_neurites
        self.max_time = max_time

    @staticmethod
    def recompute_persistence_diagrams(data_dir, morphologies, forge, persistence_diagram_location,
                                       re_download: bool):

        morphologies_subdirectory = "morphologies"
        morphologies_download_dir = os.path.join(data_dir, morphologies_subdirectory)

        def get_distribution(morphology: Resource):
            try:
                return next(d for d in morphology.distribution if d.name.endswith(".swc"))
            except StopIteration:
                return None

        distributions = dict(
            (
                encode_id_rev(m.id, m._store_metadata._rev),
                get_distribution(m)
            )
            for m in morphologies
        )

        def distribution_filename(d):
            return d.atLocation.location.split("/")[-1]

        filename_to_id = dict(
            (distribution_filename(distribution), morphology_id_rev)
            for morphology_id_rev, distribution in distributions.items()
        )

        # IF not re-downloaded it's not sure  that the locally downloaded .swc fit
        # with the current morphology revision, depends on how likely they are to be updated
        if re_download:

            # 1. Download neuron morphology content url
            logger.info("1. Download neuron morphology content url")

            logger.info(f"Downloading {len(morphologies)} morphologies to "
                        f"'{morphologies_download_dir}'...")

            os.makedirs(os.path.dirname(morphologies_download_dir), exist_ok=True)

            for m in morphologies:
                d = distributions[m.id]
                if d is not None:
                    forge.download(d, "contentUrl", path=morphologies_download_dir, overwrite=True)
                else:
                    logger.info(f">  Missing morphology file for {m.name}")

            logger.info(">  Finished downloading morphology files")
        else:
            logger.info("1. Getting local neuron morphology swc files")

        morph_files: List[str] = [
            os.path.join(morphologies_download_dir, f)
            for f in os.listdir(morphologies_download_dir) if
            os.path.isfile(os.path.join(morphologies_download_dir, f))
        ]

        # 2. Load morphologies
        logger.info("2. Load morphologies .swc files")
        morphs, errors = load_morphologies(morph_files)

        # 3. Morphology to TMD neuron
        logger.info("3. Morphology to TMD neuron")
        neurons: Dict[str, Neuron] = dict()
        failed_morphs = set()

        # dl_m = set(os.listdir(morphologies_download_dir))
        # filenames_from_resource = set(filename_to_id.keys())
        # print(filenames_from_resource.difference(dl_m))
        # print(dl_m.difference(filenames_from_resource))

        for filename, morph in morphs.items():

            morphology_id_rev = filename_to_id.get(filename, None)
            try:
                neurons[morphology_id_rev] = to_tmd_neuron(morph)
            except Exception:
                failed_morphs.add(morphology_id_rev)

        logger.info(f">  TMD computation failed for {len(failed_morphs)} morphs ({failed_morphs})")

        # 4. Persistence diagrams
        logger.info("4. Persistence diagrams")
        diagrams = dict(
            (morphology_id_rev, get_persistence_data(neuron).tolist())
            for morphology_id_rev, neuron in neurons.items()
        )

        # 5. Save persistence diagrams
        os.makedirs(os.path.dirname(persistence_diagram_location), exist_ok=True)
        with open(persistence_diagram_location, "w") as f:
            json.dump(diagrams, f)

    @staticmethod
    def get_persistence_diagrams(data_dir, re_compute: bool,
                                 re_download: bool, morphologies=None, forge_seu=None):

        if (morphologies is None or forge_seu is None) and re_compute:
            raise Exception("Missing morphologies and forge instance, cannot recompute")

        persistence_diagram_location = os.path.join(
            data_dir,
            "persistence_diagrams/"
            "persistence_diagrams_all_neurites_id_rev_index.json"
        )

        # If you need to recompute persistence diagrams (e.g. new morphs where added
        # or morphs where updated)
        if re_compute:
            TMDModel.recompute_persistence_diagrams(
                persistence_diagram_location=persistence_diagram_location,
                data_dir=data_dir,
                forge=forge_seu,
                morphologies=morphologies,
                re_download=re_download
            )

        with open(persistence_diagram_location, "r") as f:
            diagram_all_neurites = json.load(f)

        # Compute maximum death/birth time of all diagrams to know the global scale.

        all_maxes = []
        for d in diagram_all_neurites.values():
            d = np.array(d)
            all_maxes += [d[:, 0].max(), d[:, 1].max()]

        max_time = max(all_maxes)

        return diagram_all_neurites, max_time

    @staticmethod
    def plot_diagram(diagram, max_value=None):
        positive = np.array([
            [s, e]
            for s, e in diagram
            if s <= e
        ])
        negative = np.array([
            [s, e]
            for s, e in diagram
            if s >= e
        ])
        births = [el[0] for el in diagram]
        if max_value is not None:
            births.append(max_value)
        f, ax = plt.subplots(figsize=(5, 5))
        if positive.shape[0] != 0:
            ax.scatter(np.array(positive)[:, 0], np.array(positive)[:, 1])
        if negative.shape[0] != 0:
            ax.scatter(np.array(negative)[:, 0], np.array(negative)[:, 1])
        ax.plot(births, births)
        plt.show()

    @staticmethod
    def plot_diagram_profile(diagram, max_time, width, ylim):
        TMDModel.plot_diagram(diagram, max_value=max_time)
        plt.show()

        lower_points, upper_points = TMDModel.diagram_to_persistence_points(diagram)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))

        if lower_points.shape[0] != 0:
            x_vals = np.arange(0, max_time, max_time / 200)
            lower_density = TMDModel.evaluate_composed_density(lower_points, x_vals, width)
            ax1.plot(x_vals, lower_density)
            ax1.scatter(lower_points[:, 0], lower_points[:, 1])
            ax1.set_ylim((0, ylim))

        if upper_points.shape[0] != 0:
            x_vals = np.arange(0, max_time, max_time / 200)
            upper_density = TMDModel.evaluate_composed_density(upper_points, x_vals, width)
            ax2.plot(x_vals, upper_density)
            ax2.scatter(upper_points[:, 0], upper_points[:, 1])
            ax2.set_ylim((0, ylim))

        plt.show()

    @staticmethod
    def diagram_to_persistence_points(diagram):
        lower_points = np.array([
            [s, s - t] for s, t in diagram if s >= t
        ])
        upper_points = np.array([
            [s, t - s] for s, t in diagram if s <= t
        ])
        return lower_points, upper_points

    @staticmethod
    def evaluate_composed_density(points, x, width):
        centers = points[:, 0]
        masses = points[:, 1]
        return np.array([TMDModel.kernel_density(el, centers, masses, width) for el in x])

    @staticmethod
    def kernel_density(x, centers, masses, kernel_width):
        density = np.sum(
            masses * np.exp(- (2 * kernel_width) ** -2 * (x - centers) ** 2))
        return density

    @staticmethod
    def compute_persistence_vector(diagram, dim, max_time, kernel_width, max_height,
                                   visualize=False):

        # print(morph_file)
        if visualize:
            TMDModel.plot_diagram_profile(diagram, max_time, kernel_width, max_height)

        if not dim % 2:
            lower_dim = upper_dim = int(dim / 2)
        else:
            lower_dim = int(dim / 2) + 1
            upper_dim = dim - lower_dim

        lower_points, upper_points = TMDModel.diagram_to_persistence_points(diagram)

        if lower_points.shape[0] == 0:
            lower_vector = np.zeros(lower_dim)
        else:
            lower_vector = TMDModel.evaluate_composed_density(
                lower_points, np.linspace(0, max_time, num=lower_dim), kernel_width)
        if upper_points.shape[0] == 0:
            upper_vector = np.zeros(upper_dim)
        else:
            upper_vector = TMDModel.evaluate_composed_density(
                upper_points, np.linspace(0, max_time, num=upper_dim), kernel_width)
        return np.concatenate([lower_vector, upper_vector])

    @staticmethod
    def visualize_fc(x, keys):
        pca = PCA(n_components=2)
        x_2d = pca.fit_transform(x)
        # print("Explained variance: ", sum(pca.explained_variance_ratio_))

        # labels = np.array([names_to_regions[k] for k in keys])
        labels = np.array(["Batch A" for k in keys[:200]] + ["Batch B" for k in keys[200:]])
        unique_labels = sorted(list(set(labels)))
        cm = plt.get_cmap('gist_rainbow')
        generated_colors = np.array([
            cm(1. * i / len(unique_labels))
            for i in range(len(unique_labels))
        ])
        np.random.shuffle(generated_colors)

        alpha = 1
        fig, ax = plt.subplots(figsize=(7, 7))

        # create a scatter per node label
        for i, l in enumerate(unique_labels):
            indices = np.where(labels == l)
            ax.scatter(
                x_2d[indices, 0],
                x_2d[indices, 1],
                c=[generated_colors[i]] * indices[0].shape[0],
                s=50,
                label=l
            )
        ax.legend()
        plt.show()

    @staticmethod
    def rest(diagram_all_neurites, dim, visualize, max_time, kernel_width, max_height) \
            -> EmbeddingPipeline:

        vectors = dict(
            (
                morphology_id,
                TMDModel.compute_persistence_vector(
                    diagram=diagram, dim=dim, max_time=max_time, kernel_width=kernel_width,
                    max_height=max_height
                )
            )
            for morphology_id, diagram in diagram_all_neurites.items()
        )

        keys = list(vectors.keys())
        scaled_x = np.float32(np.stack([vectors[k] for k in keys]))

        #  print("Shape: ", scaled_x.shape)

        scaled_x = (scaled_x / scaled_x.max()).tolist()

        # Visualize the points in 2D

        if visualize:
            TMDModel.visualize_fc(scaled_x, keys)

        similarity_index = ScikitLearnSimilarityIndex(
            dimension=dim, similarity="euclidean",
            initial_vectors=scaled_x
        )

        point_ids = keys
        sim_processor = SimilarityProcessor(similarity_index, point_ids=point_ids)

        pipeline = EmbeddingPipeline(
            preprocessor=None,
            embedder=None,
            similarity_processor=sim_processor
        )

        return pipeline

        # Actually, for this kind of embeddings, it makes more sense to use
        # [Wasserstein metric](https://en.wikipedia.org/wiki/Wasserstein_metric).
        # See BlueGraph implementation:

        # similarity_index = ScikitLearnSimilarityIndex(
        #     dimension=dim, similarity="wasserstein",
        #     initial_vectors=scaled_x)
        # sim_processor = SimilarityProcessor(
        #     similarity_index, keys)
        # pipeline = EmbeddingPipeline(
        #     preprocessor=None,
        #     embedder=None,
        #     similarity_processor=sim_processor)


class UnscaledTMDModel(TMDModel):

    def __init__(self, model_data: ModelDataImpl):
        super().__init__(model_data)

    def run(self) -> EmbeddingPipeline:
        return TMDModel.rest(self.diagram_all_neurites, dim=256, visualize=self.visualize,
                             max_time=self.max_time, kernel_width=120, max_height=17000)


class ScaledTMDModel(TMDModel):

    def __init__(self, model_data: ModelDataImpl):
        super().__init__(model_data)

    def run(self) -> EmbeddingPipeline:
        # 5.2. Scale persistence diagrams before vectorization
        # Scale each diagram, so that the birth/death time are in the interval [0, 1].

        scaled_diagram_all_neurites = {}
        for name, diagram in self.diagram_all_neurites.items():
            diagram = np.array(diagram)
            t_min = diagram.min()
            t_max = diagram.max()
            scaled_diagram_all_neurites[name] = (diagram - t_min) / (t_max - t_min)

        return TMDModel.rest(
            scaled_diagram_all_neurites, dim=256, visualize=self.visualize,
            max_height=7, max_time=1, kernel_width=0.02
        )


unscaled_model_description = ModelDescription({
    "name": "SEU NeuronMorphology TMD-based Embedding",
    "description": "Vectorization of scaled persistence diagrams of the SEU neuron "
                   "morphologies TMD",
    "filename": "SEU_morph_TMD_euclidean",
    "label": "TMD",
    "distance": "euclidean",
    "model": UnscaledTMDModel
})

scaled_model_description = ModelDescription({
    "name": "SEU NeuronMorphology scaled TMD-based Embedding",
    "description": "SEU NeuronMorphology scaled TMD-based Embedding Vectorization of scaled "
                   "persistence diagrams of the SEU neuron morphologies TMD (scaled)",
    "filename": "SEU_morph_scaled_TMD_euclidean",
    "label": "TMD (scaled)",
    "distance": "euclidean",
    "model": ScaledTMDModel
})
