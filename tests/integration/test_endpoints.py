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

import cProfile
import pstats
from pstats import SortKey


from inference_tools.execution import apply_rule
from inference_tools.source.elastic_search import ElasticSearch

all_aspect = "https://bbp.epfl.ch/neurosciencegraph/data/abb1949e-dc16-4719-b43b-ff88dabc4cb8"

sample_neurom_seu = \
    'https://bbp.epfl.ch/neurosciencegraph/data/neuronmorphologies/8b2abe4c-190f-4595-9b1d-15256ca877f6'


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
    )
])
def test_try_rules(rule_forge, rule_id, parameters, token, forge_factory):
    # body = {
    #     "rules": [{"id": rule_id}],
    #     "inputFilter": parameters
    # }
    #
    # base_dev = "https://kg-inference-api.kcpdev.bbp.epfl.ch/"
    # base_prod = "https://kg-inference-api.kcp.bbp.epfl.ch/"
    #
    # def do(base):
    #     with cProfile.Profile() as pr:
    #
    #         response = requests.post(
    #             url=f"{base}infer",
    #             data=json.dumps(body),
    #             headers=DeltaUtils.make_header(token),
    #             verify=False
    #         )
    #
    #         res = response.json()
    #         print(len(res[0]["results"]))
    #         pstats.Stats(pr).sort_stats(SortKey.CUMULATIVE).print_stats(10)
    #
    # print("____________________ DEV ")
    # do(base_dev)
    # print("____________________ PROD ")
    # do(base_prod)

    def do_2(use_resources):
        with cProfile.Profile() as pr:
            res = apply_rule(
                forge_factory=forge_factory,
                rule=rule_forge.as_json(ElasticSearch.get_by_id(ids=rule_id, forge=rule_forge)),
                parameter_values=dict(parameters),
                premise_check=False,
                debug=False,
                use_resources=use_resources
            )

            print(len(res))
            pstats.Stats(pr).sort_stats(SortKey.CUMULATIVE).print_stats(10)

    print("____________________ LIB using forge")
    do_2(True)
    print("____________________ LIB using delta")
    do_2(False)

