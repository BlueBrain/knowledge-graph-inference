from typing import Dict, Tuple, List

import numpy as np

from pandas import concat as pandas_concat
from pandas import DataFrame

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler, MultiLabelBinarizer

from pandas import Series
from morphio._morphio import Morphology, Option, MorphioError
from tmd.Neuron.Neuron import Neuron
from tmd.Topology.methods import get_ph_neuron
from tmd.io.io import convert_morphio_trees
from tmd.utils import TREE_TYPE_DICT

from bluegraph.preprocess import ScikitLearnPGEncoder

from inference_tools.source.elastic_search import ElasticSearch

from bluegraph import PandasPGFrame

from inference_tools.bucket_configuration import NexusBucketConfiguration
from similarity_model.building.model_data import ModelData
from similarity_model.registration.logger import logger
from similarity_model.utils import encode_id_rev


class ModelDataImpl(ModelData):

    def __init__(self, src_data_dir, dst_data_dir):

        super().__init__()

        self.src_data_dir = src_data_dir
        self.dst_data_dir = dst_data_dir

        # 1. Load morphologies from Nexus
        logger.info("1. Load morphologies from Nexus")

        bucket_configuration = NexusBucketConfiguration("bbp-external", "seu", is_prod=True)
        forge_seu = allocate_forge_session(bucket_configuration)

        # ElasticSearch.set_elastic_view(forge_seu,
        #                                "https://bbp.epfl.ch/neurosciencegraph/data/test_view")
        # morphologies = ElasticSearch.get_all_documents(forge_seu)

        query = """
            SELECT ?id
            WHERE {
                ?id a NeuronMorphology ;
                    <https://bluebrain.github.io/nexus/vocabulary/deprecated> false .
            }
        """

        resources = forge_seu.sparql(query, limit=1500)
        morphologies = [forge_seu.retrieve(r.id) for r in resources]
        full_df = forge_seu.as_dataframe(morphologies, store_metadata=True)
        full_df["id"] = full_df[["id", "_rev"]].apply(lambda x: encode_id_rev(x[0], x[1]), axis=1)

        self.forge_seu = forge_seu
        self.morphologies = morphologies

        # 2. Extract neurite features
        logger.info("2. Extract neurite features")

        compartments_to_exclude: List[str] = ["ApicalDendrite"]

        get_neurite_features = lambda x: get_neurom_feature_annotations(
            data=x,
            compartments_to_exclude=compartments_to_exclude,
            statistics_of_interest=["mean", "standard_deviation"]
        )

        neurite_feature_df = DataFrame(full_df.apply(get_neurite_features, axis=1).tolist())
        self.neurite_features_df = neurite_feature_df

        # print("Including the following neurite features:")
        # for n in neurite_feature_df.columns:
        #     print("\t", n)

        # 3. Extract location-based features
        logger.info("3. Extract location based features")

        brain_region_ids = list(full_df["brainLocation.brainRegion.id"])

        get_missing_br = lambda x: get_missing_brain_region_notations(
            data=x,
            compartments_to_exclude=compartments_to_exclude,
            existing_br_notations=brain_region_ids
        )
        br_keys = full_df.apply(get_missing_br, axis=1).to_list()

        br_keys = list(set.union(*br_keys)) + brain_region_ids

        bucket_configuration = NexusBucketConfiguration(
            "neurosciencegraph", "datamodels", is_prod=True,
            elastic_search_view="https://bbp.epfl.ch/neurosciencegraph/data/views/es/dataset")

        forge_datamodels = bucket_configuration.allocate_forge_session()

        brain_region_notation = get_brain_region_notation(br_keys, forge_datamodels)

        get_location_features = lambda x: get_location_feature_annotations(
            data=x,
            compartments_to_exclude=compartments_to_exclude,
            brain_region_notation=brain_region_notation
        )

        localisation_features = DataFrame(full_df.apply(get_location_features, axis=1).tolist())
        self.localisation_features = localisation_features

        # 4. Build a data frame with the rest of the meta-data
        logger.info("4. Build a data frame with the rest of the meta-data")

        morphologies_df: DataFrame = full_df[[
            "id",
            "brainLocation.brainRegion.id",
            "somaNumberOfPoints.@value"
        ]]

        # Replace id with notation
        morphologies_df.loc[:, 'brainLocation.brainRegion.id'] = \
            morphologies_df["brainLocation.brainRegion.id"].apply(
                lambda x: brain_region_notation[x][0]
            )

        self.morphologies_df = morphologies_df
        self.full_df = full_df

        # For recomputing of persistence diagrams
        self.morphologies = morphologies
        self.forge = forge_seu

        self.frame = self.build_frame()

    def build_frame(self):

        nodes = pandas_concat(
            [
                self.morphologies_df,
                self.localisation_features
            ], axis=1).rename(columns={"id": "@id"}).set_index("@id")

        frame = PandasPGFrame()
        frame._nodes = nodes
        numerical_props = ['somaNumberOfPoints.value']

        for column in nodes.columns:
            if column != "@type":
                if column not in numerical_props:
                    try:
                        frame.node_prop_as_category(column)
                    except ValueError:
                        pass
                else:
                    frame.node_prop_as_numeric(column)

        frame.rename_node_properties({
            p: p.replace(".", "_")
            for p in frame.node_properties()
        })

        frame._nodes["BasalDendrite_Leaf_Regions"] = \
            frame._nodes["BasalDendrite_Leaf_Regions"].apply(
                lambda x: [] if isinstance(x, float) else x
            )

        frame._nodes["Axon_Leaf_Regions"] = \
            frame._nodes["Axon_Leaf_Regions"].apply(
                lambda x: [] if isinstance(x, float) else x
            )

        return frame





def explain_property_coordinates(encoder: ScikitLearnPGEncoder, graph):
    def get_encoder_features(prop_name, node_encoder, last_index):
        if node_encoder is None or isinstance(node_encoder, StandardScaler):
            return {last_index: f"{prop_name}_IDENTITY"}, last_index + 1
        if isinstance(node_encoder, TfidfVectorizer):
            return (
                {
                    i + last_index: f"{prop_name}_WORD_{f}"
                    for i, f in enumerate(node_encoder.get_feature_names_out())
                },
                last_index + len(node_encoder.get_feature_names_out())
            )
        elif isinstance(node_encoder, MultiLabelBinarizer):
            return (
                {
                    i + last_index: f"{prop_name}_CLASS_{c}"
                    for i, c in enumerate(node_encoder.classes_)
                },
                last_index + len(node_encoder.classes_)
            )

        else:
            return {}, last_index

    last_index_i = 0
    property_coordinates = {}

    node_encoders = encoder._node_encoders

    for p in graph.node_properties():
        if p in node_encoders:
            res, new_index = get_encoder_features(p, node_encoders[p], last_index_i)
            property_coordinates.update(res)
            last_index_i = new_index

    return property_coordinates


def get_neurom_feature_annotations(
        data: Series,
        compartments_to_exclude: List[str],
        statistics_of_interest: List[str]
):
    record = {}
    try:
        for ann in data.annotation:
            if "MType:Annotation" in ann["type"]:
                record["MType"] = ann["hasBody"]["label"]
            if "NeuronMorphologyFeatureAnnotation" in ann["type"]:
                compartment = ann["compartment"]
                if compartment not in compartments_to_exclude:
                    for feature_ann in ann["hasBody"]:
                        if "NeuriteLocationFeature" not in feature_ann["type"]:
                            feature_name = feature_ann["isMeasurementOf"]["label"].replace(" ", "_")

                            series = get_series(feature_ann)

                            for el in series:
                                if "statistic" in el:
                                    stat = el["statistic"].replace(" ", "_")
                                    if stat in statistics_of_interest and "value" in el:
                                        record[f"{compartment}_{stat}_{feature_name}"] = el["value"]
    except TypeError:
        pass
    return record


def get_brain_region_notation(br_ids, forge):
    brain_region_resources = ElasticSearch.get_by_ids(br_ids, forge)

    return {
        r.id: (r.notation, r.prefLabel)
        for r in brain_region_resources
    }


def get_series(feature_annotation: Dict):
    tmp = feature_annotation["series"]

    if isinstance(tmp, list):
        return tmp

    if isinstance(tmp, dict):
        return [tmp] if bool(tmp) else []

    raise Exception("Unexpected series")


def get_missing_brain_region_notations(
        data: Series,
        compartments_to_exclude: List[str],
        existing_br_notations: List[str]
):
    def get_br_in_annotation(annotation):
        location_features = [
            feature_ann for feature_ann in annotation["hasBody"]
            if "NeuriteLocationFeature" in feature_ann["type"]
        ]

        def br_id(series_element):
            return series_element["brainRegion"]["id"]

        return [
            br_id(series_item)
            for feature_ann in location_features
            for series_item in get_series(feature_ann)
            if br_id(series_item) is not None
        ]

    valid_annotation = [
        ann for ann in data.annotation
        if "NeuronMorphologyFeatureAnnotation" in ann["type"]
           and ann["compartment"] not in compartments_to_exclude
    ]

    return set([e for ann in valid_annotation for e in get_br_in_annotation(ann)
                if e not in existing_br_notations])


def get_location_feature_annotations(
        data: Series,
        compartments_to_exclude: List[str],
        brain_region_notation: Dict[str, Tuple[str, str]]
):
    record = {}

    try:
        for ann in data.annotation:
            if "MType:Annotation" in ann["type"]:
                record["MType"] = ann["hasBody"]["label"]
            if "NeuronMorphologyFeatureAnnotation" in ann["type"] \
                    and ann["compartment"] not in compartments_to_exclude:

                compartment = ann["compartment"]

                location_features = [feature_ann for feature_ann in ann["hasBody"]
                                     if "NeuriteLocationFeature" in feature_ann["type"]]

                for feature_ann in location_features:
                    feature_name = feature_ann["isMeasurementOf"]["label"].replace(" ", "_")

                    series = get_series(feature_ann)

                    regions = sum([
                        [brain_region_notation[r["brainRegion"]["id"]][0]] * r["count"]
                        for r in series
                    ], [])

                    record[f"{compartment}_{feature_name}"] = regions

    except TypeError as e:
        print(e)
        pass

    return record


def load_morphologies(paths: List[str], progress_bar=iter) -> \
        Tuple[Dict[str, Morphology], Dict[str, str]]:
    """Author: Stanislav Schmidt"""
    morphs = {}
    errors = {}
    for file in progress_bar(paths):
        filename = file.split("/")[-1]
        try:
            morphology = Morphology(file, Option.soma_sphere)
        except MorphioError as exc:
            errors[filename] = str(exc)
        else:
            morphs[filename] = morphology

    return morphs, errors


def to_tmd_neuron(morphology: Morphology) -> Neuron:
    """
    Convert a MorphIO neuron to a TMD neuron.

    Author: Stanislav Schmidt
    """

    neuron = Neuron()
    for tree in convert_morphio_trees(morphology):
        neuron.append_tree(tree, TREE_TYPE_DICT)

    return neuron


# TODO where is neurite_type coming from, get_persistence_data only called with 1 param,
#  and initially did not have a neurite_type function parameter ?
def get_persistence_data(neuron, neurite_type=None):
    """Author: Stanislav Schmidt"""
    if neurite_type is None:
        kind = "all"
    else:
        kind = neurite_type.name
    ph = get_ph_neuron(neuron, neurite_type=kind)
    ph_arr = np.array(ph)
    return ph_arr


def get_frame_nodes(frame: PandasPGFrame):
    return frame._nodes


