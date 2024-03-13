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

from inference_tools.execution import apply_rule


def test_infer(query_conf, forge_factory):
    q = {
        "@type": "SparqlQuery",
        "hasBody": {"query_string": ""},
        "hasParameter": [],
        "queryConfiguration": query_conf,
        "resultParameterMapping": []
    }

    rule_dict = {
        "@id": "test",
        "@type": "DataGeneralizationRule",
        "description": "Test Rule description",
        "name": "Test rule",
        "searchQuery": q,
        "targetResourceType": "Entity"
    }

    test = apply_rule(forge_factory=forge_factory, parameter_values={}, rule=rule_dict)
