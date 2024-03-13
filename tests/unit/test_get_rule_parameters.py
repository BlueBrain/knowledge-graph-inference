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

from inference_tools.datatypes.rule import Rule

from inference_tools.utils import get_search_query_parameters


@pytest.fixture
def rule1_dict(query_conf):
    return {
        "@id": "id_value",
        "@type": "DataGeneralizationRule",
        "description": "Test rule desc",
        "name": "Test rule",
        "searchQuery": {
            "@type": "QueryPipe",
            "head": {
                "@type": "SparqlQuery",
                "hasBody": {"query_string": ""},
                "hasParameter": [
                    {"@type": "path", "name": "param1"},
                    {"@type": "sparql_list", "name": "param2"},
                    {"@type": "path", "name": "param3"},
                ],
                "queryConfiguration": query_conf,
                "resultParameterMapping": [
                    {"parameterName": "param4", "path": "id"}
                ]
            },
            "rest": {
                "@type": "QueryPipe",
                "head": {
                    "@type": "SparqlQuery",
                    "hasBody": {"query_string": ""},
                    "hasParameter": [
                        {"@type": "path", "name": "param4"},
                        {"@type": "sparql_list", "name": "param5"},
                        {"@type": "path", "name": "param6"},
                    ],
                    "queryConfiguration": query_conf,
                    "resultParameterMapping": [
                        {"parameterName": "param7", "path": "id"}
                    ]
                },
                "rest": {
                    "@type": "SparqlQuery",
                    "hasBody": {"query_string": ""},
                    "hasParameter": [
                        {"@type": "sparql_list", "name": "param7"},
                        {"@type": "path", "name": "param8"},
                        {"@type": "MultiPredicateObjectPair", "name": "param9"},
                        {"@type": "path", "name": "param10"}
                    ],
                    "queryConfiguration": query_conf
                }
            }
        },
        "targetResourceType": "Entity"
    }


@pytest.fixture
def rule3(rule1_dict):
    rule3_dict = rule1_dict.copy()
    rule3 = Rule(rule3_dict)
    rule3.search_query.head.result_parameter_mapping = []
    rule3.search_query.rest.head.result_parameter_mapping = []
    return rule3


@pytest.fixture
def rule2(rule1_dict):
    rule2_dict = rule1_dict.copy()
    rule2 = Rule(rule2_dict)
    rule2.search_query.head.result_parameter_mapping = []
    return rule2


@pytest.fixture
def rule1(rule1_dict):
    return Rule(rule1_dict)


@pytest.mark.parametrize("rule_string, expected_parameters", [
    pytest.param(
        "rule1",
        [
            f"param{i}" for i in range(1, 11)
            if i not in [4, 7]
        ],
        id="rule1"
    ),
    pytest.param(
        "rule2",
        [
            f"param{i}" for i in range(1, 11)
            if i != 7
        ],
        id="rule2"
    ),
    pytest.param(
        "rule3",
        [f"param{i}" for i in range(1, 11)],
        id="rule3"
    )
])
def test_get_search_query_parameters(rule_string, expected_parameters, request):
    rule = request.getfixturevalue(rule_string)
    assert (expected_parameters == list(get_search_query_parameters(rule).keys()))
