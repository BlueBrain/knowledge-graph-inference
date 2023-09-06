from typing import List

from inference_tools.datatypes.query import SimilaritySearchQuery
from inference_tools.datatypes.rule import Rule
from inference_tools.nexus_utils.bucket_configuration import NexusBucketConfiguration
from inference_tools.rules import fetch_rules
from inference_tools.similarity.main import SIMILARITY_MODEL_SELECT_PARAMETER_NAME


def test_fetch_by_resource_id():

    rule_forge = NexusBucketConfiguration("bbp", "inference-rules", True).allocate_forge_session()
    rule_dm = NexusBucketConfiguration("neurosciencegraph", "datamodels", True).allocate_forge_session()
    rule_view = "https://bbp.epfl.ch/neurosciencegraph/data/rule_view_no_tag"

    public_hippocampus_nm = "https://bbp.epfl.ch/neurosciencegraph/data/neuronmorphologies/402ba796-81f4-460c-870e-98e8fb1bd982"
    bbp_external_nm = "https://bbp.epfl.ch/neurosciencegraph/data/neuronmorphologies/608c996a-15a3-4d8a-aa4a-827fa6946f9b"
    public_thalamus_nm = "https://bbp.epfl.ch/neurosciencegraph/data/b7388c82-8c59-4454-beb3-6fb59d0d992d"
    bbp_mouselight_nm = "https://bbp.epfl.ch/neurosciencegraph/data/neuronmorphologies/0c78e043-4882-4d5d-980d-a94953433398"

    nm = {
        "public/hippocampus": public_hippocampus_nm,
        "public/thalamus": public_thalamus_nm,
        "bbp/mouselight": bbp_mouselight_nm,
        "bbp-external/seu": bbp_external_nm
    }

    values = {
      "Axon_co-projection-based_embedding": "https://bbp.epfl.ch/data/bbp/atlas/55873d57-622e-4125-b135-2ab4ec63d443",
      "Brain_region_BBP_ontology-based_embedding_-_BMO_File": "https://bbp.epfl.ch/data/bbp/atlas/4254dd62-3467-4b32-a077-90b1aff3208e",
      "Coordinates-based_embedding": "https://bbp.epfl.ch/data/bbp/atlas/35115cdb-68fc-4ded-8ae2-819c8027e50f",
      "Dendrite_co-projection-based_embedding": "https://bbp.epfl.ch/data/bbp/atlas/1ec83f1d-eff3-474a-9d8d-be38888d282d",
      "Unscaled_TMD-based_embedding": "https://bbp.epfl.ch/data/bbp/atlas/722fd3d9-04cb-4577-8bb2-57a7bc8401c1"
    }

    values = list(values.items())

    nm_to_expected = {
        "public/hippocampus": [4],
        "public/thalamus": [1, 4],
        "bbp/mouselight": [1, 4],
        "bbp-external/seu": [0, 1, 2, 3, 4]
    }

    for label, nm in nm.items():
        test: List[Rule] = fetch_rules(rule_forge, rule_dm, rule_view, resource_id=nm)

        for rule in test:
            if isinstance(rule.search_query, SimilaritySearchQuery):

                parameter_values = next(
                    e.values for e in rule.search_query.parameter_specifications
                    if e.name == SIMILARITY_MODEL_SELECT_PARAMETER_NAME
                )
                qc_names = [
                    qc.embedding_model_data_catalog.name
                    for qc in rule.search_query.query_configurations
                ]

                expected_parameter_values = dict(values[i] for i in nm_to_expected[label])

                expected_qc_names = set(list(expected_parameter_values.keys()))
                computed_qc_names = set(list(map(lambda x: x.replace(" ", "_"), qc_names)))

                assert len(computed_qc_names.difference(expected_qc_names)) == 0
                assert expected_parameter_values == parameter_values


