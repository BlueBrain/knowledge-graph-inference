from typing import List, Dict, Optional

from inference_tools.datatypes.query import SimilaritySearchQuery
from inference_tools.datatypes.rule import Rule
from inference_tools.rules import fetch_rules
from inference_tools.similarity.main import SIMILARITY_MODEL_SELECT_PARAMETER_NAME
from inference_tools.type import RuleType, QueryType


def test_fetch_by_resource_id_lots(forge_factory, rule_forge):

    ids = []
    for bucket in ["bbp-external/seu", "bbp/mouselight"]:
        org, project = bucket.split("/")
        forge = forge_factory(org, project)
        nms = forge.search({"type": "NeuronMorphology"}, limit=10)
        ids.extend([nm.id for nm in nms])

    # with cProfile.Profile() as pr:
    test: Dict[str, List[Rule]] = fetch_rules(
        rule_forge, resource_ids=ids
    )

    for res_id, list_rules in test.items():

        print(res_id, "Rule count:", len(list_rules))

        for rule in list_rules:
            if isinstance(rule.search_query, SimilaritySearchQuery):
                print(
                    rule.id,
                    "Model length",
                    len(next(e for e in rule.search_query.parameter_specifications
                             if e.name == "SelectModelsParameter").values.keys()),
                    len([e.embedding_model_data_catalog.name for e in
                         rule.search_query.query_configurations])
                )

        # pstats.Stats(pr).sort_stats(SortKey.CUMULATIVE).print_stats(10)


def test_fetch_by_resource_id(rule_forge):

    public_hippocampus_nm = "https://bbp.epfl.ch/neurosciencegraph/data/neuronmorphologies/402ba796-81f4-460c-870e-98e8fb1bd982"
    bbp_external_nm = "https://bbp.epfl.ch/neurosciencegraph/data/neuronmorphologies/608c996a-15a3-4d8a-aa4a-827fa6946f9b"
    public_thalamus_nm = "https://bbp.epfl.ch/neurosciencegraph/data/b7388c82-8c59-4454-beb3-6fb59d0d992d"
    bbp_mouselight_nm = "https://bbp.epfl.ch/neurosciencegraph/data/neuronmorphologies/0c78e043-4882-4d5d-980d-a94953433398"

    values = {
        "Axon_co-projection-based_similarity": "https://bbp.epfl.ch/data/bbp/atlas/55873d57-622e-4125-b135-2ab4ec63d443",
        "Coordinates-based_similarity": "https://bbp.epfl.ch/data/bbp/atlas/35115cdb-68fc-4ded-8ae2-819c8027e50f",
        "Unscaled_Topology_Morphology_Descriptor-based_similarity": "https://bbp.epfl.ch/data/bbp/atlas/722fd3d9-04cb-4577-8bb2-57a7bc8401c1"
    }

    values = list(values.items())

    nm_to_expected = {
        public_hippocampus_nm: [2],
        public_thalamus_nm: [2],
        bbp_mouselight_nm: [2],
        bbp_external_nm: [0, 1, 2]
    }

    test: Dict[str, List[Rule]] = fetch_rules(
        rule_forge, resource_ids=list(nm_to_expected.keys())
    )

    for res_id, list_rules in test.items():

        for rule in list_rules:

            if isinstance(rule.search_query, SimilaritySearchQuery) and \
                    rule.search_query.type == QueryType.SIMILARITY_QUERY:

                parameter_values = next(
                    e.values for e in rule.search_query.parameter_specifications
                    if e.name == SIMILARITY_MODEL_SELECT_PARAMETER_NAME
                )

                qc_names = [
                    qc.embedding_model_data_catalog.name
                    for qc in rule.search_query.query_configurations
                ]

                expected_parameter_values = dict(values[i] for i in nm_to_expected[res_id])

                expected_qc_names = set(list(expected_parameter_values.keys()))
                computed_qc_names = set(list(map(lambda x: x.replace(" ", "_"), qc_names)))

                assert len(computed_qc_names.difference(expected_qc_names)) == 0


def test_fetch_by_rule_type(rule_forge):

    type_to_expected_count = [
        ([RuleType.EmbeddingBasedGeneralizationRule], 2),
        ([RuleType.HierarchyBasedGeneralizationRule], 1),
        ([RuleType.DataGeneralizationRule], 3),
        ([RuleType.ResourceGeneralizationRule], 1),
        ([RuleType.EmbeddingBasedGeneralizationRule, RuleType.HierarchyBasedGeneralizationRule], 3),
        ([RuleType.EmbeddingBasedGeneralizationRule, RuleType.ResourceGeneralizationRule], 2),
        ([], 3),
        (None, 3)
    ]

    for types, count in type_to_expected_count:
        test = fetch_rules(
            rule_forge, rule_types=types
        )
        assert len(test) == count


def test_fetch_by_resource_type(rule_forge):

    test = fetch_rules(
        rule_forge, resource_types=["NeuronMorphology"]
    )
    assert len(test) == 3


