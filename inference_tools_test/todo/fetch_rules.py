from typing import List
import os

from kgforge.core import KnowledgeGraphForge
from inference_tools.datatypes.query import SimilaritySearchQuery
from inference_tools.datatypes.rule import Rule
from inference_tools.rules import fetch_rules
from inference_tools.similarity.main import SIMILARITY_MODEL_SELECT_PARAMETER_NAME
from inference_tools.type import RuleType


def _get_test_forge():
    es_rule_view = "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-es/rule_view_no_tag"
    sparql_rule_view = "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-sp/rule_view_no_tag"

    bucket = "bbp/inference-rules"
    endpoint = "https://bbp.epfl.ch/nexus/v1"

    token_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "./token.txt")

    with open(token_file_path, encoding="utf-8") as f:
        token = f.read()

    config = "https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/notebooks/use-cases/prod-forge-nexus.yml"
    args = dict(
        configuration=config,
        endpoint=endpoint,
        token=token,
        bucket=bucket,
        debug=False,
        searchendpoints={
            "elastic": {"endpoint": es_rule_view},
            "sparql": {"endpoint": sparql_rule_view}
        }
    )
    return KnowledgeGraphForge(**args)


def test_fetch_by_resource_id():
    rule_forge = _get_test_forge()

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
        test: List[Rule] = fetch_rules(
            rule_forge, resource_id=nm
        )

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


def test_fetch_by_rule_type():
    rule_forge = _get_test_forge()

    type_to_expected_count = [
        ([RuleType.EmbeddingBasedGeneralizationRule], 2),
        ([RuleType.HierarchyBasedGeneralizationRule], 1),
        ([RuleType.DataGeneralizationRule], 3),
        ([RuleType.ResourceGeneralizationRule], 1),
        ([RuleType.EmbeddingBasedGeneralizationRule, RuleType.HierarchyBasedGeneralizationRule], 3),
        ([RuleType.EmbeddingBasedGeneralizationRule, RuleType.ResourceGeneralizationRule], 2),
        ([], 3),
        (None, 3),
    ]

    for types, count in type_to_expected_count:
        test = fetch_rules(
            rule_forge, rule_types=types
        )
        assert len(test) == count


def test_fetch_by_resource_type():
    rule_forge = _get_test_forge()

    test = fetch_rules(
        rule_forge, resource_types=["NeuronMorphology"]
    )
    assert len(test) == 3


# test_fetch_by_resource_id()
# test_fetch_by_resource_type()
test_fetch_by_rule_type()
