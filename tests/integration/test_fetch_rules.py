# This file is part of knowledge-graph-inference.
# Copyright 2024 Blue Brain Project / EPFL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
        rule_forge, resource_ids=ids, forge_factory=forge_factory
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


def test_fetch_by_resource_id(rule_forge, forge_factory):

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
        rule_forge, resource_ids=list(nm_to_expected.keys()), forge_factory=forge_factory
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
        ([RuleType.EMBEDDING_BASED_GENERALIZATION_RULE], 2),
        ([RuleType.HIERARCHY_BASED_GENERALIZATION_RULE], 1),
        ([RuleType.DATA_GENERALIZATION_RULE], 3),
        ([RuleType.RESOURCE_GENERALIZATION_RULE], 1),
        ([RuleType.EMBEDDING_BASED_GENERALIZATION_RULE, RuleType.HIERARCHY_BASED_GENERALIZATION_RULE], 3),
        ([RuleType.EMBEDDING_BASED_GENERALIZATION_RULE, RuleType.RESOURCE_GENERALIZATION_RULE], 2),
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


def test_non_similarity_based_rule_format_resource_id_provided(rule_forge, forge_factory):
    resource_id = "https://bbp.epfl.ch/neurosciencegraph/data/neuronmorphologies/97340ac1-d5ed-48b9-a4ff-ff0323e91a8f"

    rules = fetch_rules(
        rule_forge, resource_ids=[resource_id], forge_factory=forge_factory
    )

    rules_tied_to_id = rules[resource_id]

    if len(rules_tied_to_id) > 0:  # Not true in all environments, due to rule tagging
        assert all(r.flattened_input_parameters for r in rules_tied_to_id)
