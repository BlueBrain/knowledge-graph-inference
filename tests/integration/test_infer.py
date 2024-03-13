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

import pytest

import json
import cProfile
import pstats
import os
from pstats import SortKey

from inference_tools.execution import apply_rule
from inference_tools.source.elastic_search import ElasticSearch

all_aspect = "https://bbp.epfl.ch/neurosciencegraph/data/abb1949e-dc16-4719-b43b-ff88dabc4cb8"

sample_neurom_seu = \
    'https://bbp.epfl.ch/neurosciencegraph/data/neuronmorphologies/8b2abe4c-190f-4595-9b1d-15256ca877f6'
sample_neurom_public_thalamus = \
    "https://bbp.epfl.ch/neurosciencegraph/data/43edd8bf-5dfe-45cd-b6d8-1a604dd6beca"


@pytest.mark.parametrize("rule_id, parameters", [
    pytest.param(
        all_aspect, {
            'TargetResourceParameter': sample_neurom_seu,
            'SelectModelsParameter': [
                "Unscaled_Topology_Morphology_Descriptor-based_similarity",
                "Axon_co-projection-based_similarity",
                "Coordinates-based_similarity"
            ],
            'LimitQueryParameter': 100
        },
        id="param1",
    ),
    pytest.param(
        all_aspect, {
            'TargetResourceParameter': sample_neurom_public_thalamus,
            'SelectModelsParameter': ["Unscaled_Topology_Morphology_Descriptor-based_similarity"],
            'LimitQueryParameter': 100
        },
        id="param1",
    ),
    pytest.param(
        "https://bbp.epfl.ch/neurosciencegraph/data/5d04995a-6220-4e82-b847-8c3a87030e0b",{
            "GeneralizedFieldValue": "http://api.brain-map.org/api/v2/data/Structure/315",
            "GeneralizedFieldName": "BrainRegion",
            "TypeQueryParameter": "https://neuroshapes.org/Trace",
            "LimitQueryParameter": 10,
            "HierarchyRelationship": "SubclassOf",
            "SearchDirectionBlock": "Down",
            "PathToGeneralizedField": "BrainRegion"
        },
        id="param2",
    ),
    pytest.param(
        "https://bbp.epfl.ch/neurosciencegraph/data/9d64dc0d-07d1-4624-b409-cdc47ccda212", {
            "BrainRegionQueryParameter": "http://api.brain-map.org/api/v2/data/Structure/375",
            "TypeQueryParameter": "https://neuroshapes.org/NeuronMorphology"
        },
        id="param3",
    )
])
def test_try_rules(rule_forge, rule_id, parameters, forge_factory):

    print(parameters)

    with cProfile.Profile() as pr:

        rule = rule_forge.as_json(ElasticSearch.get_by_id(ids=rule_id, forge=rule_forge))

        res = apply_rule(
            forge_factory=forge_factory,
            rule=rule,
            parameter_values=dict(parameters),
            premise_check=False,
            debug=False
        )

        print(len(res))
        pstats.Stats(pr).sort_stats(SortKey.CUMULATIVE).print_stats(10)


@pytest.mark.parametrize("parameters", [
    pytest.param(
        {
            'TargetResourceParameter': "https://bbp.epfl.ch/entity_1",
            'SelectModelsParameter': ["Test_Model"],
            'SpecifiedTargetResourceType': "Type1"
        },
        id="param1",
    ),
    pytest.param(
        {
            'TargetResourceParameter': "https://bbp.epfl.ch/entity_1",
            'SelectModelsParameter': ["Test_Model"]
        },
        id="param1",
    )
])
def test_fake_rule(forge_factory, parameters):

    with open(
            os.path.join(os.path.dirname(os.path.realpath(__file__)), "../data/fake_rule.json"),
            "r"
    ) as f:
        fake_rule = json.loads(f.read())

    res = apply_rule(
        forge_factory=forge_factory,
        rule=fake_rule,
        parameter_values=dict(parameters),
        premise_check=False, debug=False
    )

    forge = forge_factory("dke", "test")

    if "SpecifiedTargetResourceType" in parameters:
        type_ = parameters["SpecifiedTargetResourceType"]
    else:
        type_ = fake_rule["targetResourceType"]

    assert all(type_ in forge.retrieve(res_i["id"]).type for res_i in res)
