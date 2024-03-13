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

from contextlib import nullcontext as does_not_raise

from inference_tools.datatypes.query import Query, query_factory
from inference_tools.exceptions.exceptions import InferenceToolsException, InvalidValueException
from inference_tools.execution import get_limit

from inference_tools.source.source import DEFAULT_LIMIT
from inference_tools.type import ParameterType
from inference_tools.utils import format_parameters

field_name = "ValueField"


@pytest.mark.parametrize("parameter_values, expected_limit ", [
    pytest.param(
        {}, DEFAULT_LIMIT, id="default",
    ),
    pytest.param(
        {"LimitQueryParameter": 50}, 50, id="input",
    )
])
def test_get_limit(query_conf, forge, parameter_values, expected_limit):

    limit = get_limit(parameter_values)

    assert limit == expected_limit


@pytest.mark.parametrize(
    "parameter_values, expectation",
    [
        ({"MandatoryField": ["a", "b"]}, does_not_raise()),
        ({}, pytest.raises(InferenceToolsException)),
    ],
)
def test_parameter_format_missing_mandatory(query_conf, forge, parameter_values, expectation):
    q = {
        "@type": "SparqlQuery",
        "hasBody": {"query_string": ""},
        "hasParameter": [
            {
                "@type": "sparql_list",
                "description": "test field",
                "name": "MandatoryField",
                "optional": False
            }
        ],
        "queryConfiguration": query_conf,
        "resultParameterMapping": []
    }

    query: Query = query_factory(q)

    with expectation:
        formatted_parameters = format_parameters(
            query=query, parameter_values=parameter_values, forge=forge
        )
        assert isinstance(formatted_parameters, dict)
        assert len(formatted_parameters) != 0


@pytest.mark.parametrize("type_, expected_value ", [
    pytest.param(
        ParameterType.SPARQL_LIST.value, '(<a>, <b>)',
        id="param1",
    ),
    pytest.param(
        ParameterType.LIST.value, '"a", "b"',
        id="param2",
    ),
    pytest.param(
        ParameterType.SPARQL_VALUE_LIST.value, '("a")\n("b")',
        id="param3",
    ),
    pytest.param(
        ParameterType.SPARQL_VALUE_URI_LIST.value,
        "(<https://bbp.epfl.ch/nexus/v1/resources/bbp/atlas/_/a>)\n"
        "(<https://bbp.epfl.ch/nexus/v1/resources/bbp/atlas/_/b>)",
        id="param4",
    ),
    pytest.param(
        ParameterType.URI_LIST.value,
        "<https://bbp.epfl.ch/nexus/v1/resources/bbp/atlas/_/a>, "
        "<https://bbp.epfl.ch/nexus/v1/resources/bbp/atlas/_/b>",
        id="param5",
    )
])
def test_parameter_format_list_formatting(query_conf, forge, type_, expected_value):
    field_name = "ListField"
    parameter_values = {field_name: ["a", "b"]}

    q = {
        "@type": "SparqlQuery",
        "hasBody": {"query_string": ""},
        "hasParameter": [
            {
                "@type": type_,
                "description": "test field",
                "name": field_name,
                "optional": False
            }
        ],
        "queryConfiguration": query_conf,
        "resultParameterMapping": []
    }

    formatted_parameters = format_parameters(
        query=query_factory(q), parameter_values=parameter_values,
        forge=forge
    )

    assert formatted_parameters == {field_name: expected_value}


@pytest.mark.parametrize("type_, values, expected_value, expectation", [
    pytest.param(
        ParameterType.URI.value, {field_name: "a"},
        {field_name: 'https://bbp.epfl.ch/nexus/v1/resources/bbp/atlas/_/a'}, does_not_raise(),
        id="param1",
    ),
    pytest.param(
        ParameterType.STR.value, {field_name: "a"}, {field_name: '"a"'}, does_not_raise(),
        id="param2",
    ),
    pytest.param(
        ParameterType.PATH.value, {field_name: "a"}, {field_name: 'a'}, does_not_raise(),
        id="param3",
    ),
    pytest.param(
        ParameterType.BOOL.value, {field_name: "true"}, {field_name: "true"}, does_not_raise(),
        id="param4",
    ),
    pytest.param(
        ParameterType.BOOL.value, {field_name: "false"}, {field_name: "false"}, does_not_raise(),
        id="param5",
    ),
    pytest.param(
        ParameterType.BOOL.value, {field_name: "True"}, {field_name: "true"}, does_not_raise(),
        id="param6",
    ),
    pytest.param(
        ParameterType.BOOL.value, {field_name: "False"}, {field_name: "false"}, does_not_raise(),
        id="param7",
    ),
    pytest.param(
        ParameterType.BOOL.value, {field_name: "idk"}, None, pytest.raises(InvalidValueException),
        id="param8",
    )
])
def test_parameter_format_value_formatting(
        query_conf, forge, type_, values, expected_value, expectation
):
    def run_formatting(field_type, parameter_values):
        q = {
            "@type": "SparqlQuery",
            "hasBody": {"query_string": ""},
            "hasParameter": [
                {
                    "@type": field_type,
                    "description": "test field",
                    "name": field_name,
                    "optional": False
                }
            ],
            "queryConfiguration": query_conf,
            "resultParameterMapping": []
        }

        params = format_parameters(
            query=query_factory(q), parameter_values=parameter_values,
            forge=forge
        )

        return params

    with expectation:
        formatted_parameters = run_formatting(type_, values)
        assert formatted_parameters == expected_value

