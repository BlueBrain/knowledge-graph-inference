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

from inference_tools.exceptions.exceptions import IncompleteObjectException, \
    MissingPremiseParameterValue, InferenceToolsException

from inference_tools.datatypes.parameter_specification import ParameterSpecification
from inference_tools.type import QueryType, ParameterType, PremiseType
from inference_tools.utils import _build_parameter_map

from contextlib import nullcontext as does_not_raise

param_name = "param1"


def make_spec(name: str, type_: str, optional: bool = False, values=None):
    if values is None:
        values = {}

    return ParameterSpecification(obj={
        "name": name,
        "type": type_,
        "optional": optional,
        "values": values
    })


@pytest.fixture
def parameter_spec1():
    return [make_spec(
        name=param_name,
        type_=ParameterType.PATH.value,
        values={
            "a": "aaa",
            "b": "bbb",
            "d": "ddd",
            "e": "eee"
        }
    )]


@pytest.fixture
def parameter_spec2(parameter_spec1):
    parameter_spec2 = parameter_spec1.copy()
    parameter_spec2[0].type = ParameterType.SPARQL_VALUE_LIST
    return parameter_spec2


@pytest.fixture
def parameter_spec3():
    return []


@pytest.fixture
def parameter_spec4():
    return [make_spec(name=param_name, type_=ParameterType.PATH.value, optional=True)]


@pytest.mark.parametrize(
    "parameter_values, parameter_spec_str, expected_parameter_map, expectation",
    [
        pytest.param(
            {},
            "parameter_spec3",
            {},
            does_not_raise(),
            id="nothing_no_spec"
        ),
        pytest.param(
            {},
            "parameter_spec4",
            {},
            does_not_raise(),
            id="nothing_optional_true"
        ),
        pytest.param(
            {param_name: "a"},
            "parameter_spec1",
            {param_name: "aaa"},
            does_not_raise(),
            id="one_value_no_list"
        ),
        pytest.param(
            {param_name: ["a", "e"]},
            "parameter_spec1",
            {param_name: 'aaa'},
            does_not_raise(),
            id="two_values_no_list"
        ),
        pytest.param(
            {param_name: ["a", "e"]},
            "parameter_spec2",
            {param_name: '("aaa")\n("eee")'},
            does_not_raise(),
            id="two_values_list"
        ),
        pytest.param(
            {param_name: "c"},
            "parameter_spec1",
            None,
            pytest.raises(
                InferenceToolsException,
                match=f"Invalid value for parameter {param_name}"
            ),
            id="except"
        )
    ]
)
def test_build_parameter_map(
        forge, parameter_spec_str, parameter_values, expected_parameter_map, request, expectation
):
    parameter_spec = request.getfixturevalue(parameter_spec_str)

    with expectation:
        assert _build_parameter_map(
            forge=forge,
            parameter_spec=parameter_spec,
            parameter_values=parameter_values,
            query_type=QueryType.SPARQL_QUERY
        ) == expected_parameter_map


def test_build_parameter_map_missing_values(forge):
    parameter_spec = [make_spec(name=param_name, type_=ParameterType.PATH.value)]
    parameter_values = {}

    expected_msg = \
        f"The parameter {param_name} has been created with missing mandatory information: value"

    with pytest.raises(IncompleteObjectException, match=expected_msg):
        _build_parameter_map(
            forge=forge,
            parameter_spec=parameter_spec,
            parameter_values=parameter_values,
            query_type=QueryType.SPARQL_QUERY
        )

    expected_msg2 = 'Premise cannot be ran because parameter param1 has not been provided'
    # Different error raised if dealing with premises
    with pytest.raises(MissingPremiseParameterValue, match=expected_msg2):
        _build_parameter_map(
            forge=forge,
            parameter_spec=parameter_spec,
            parameter_values=parameter_values,
            query_type=PremiseType.SPARQL_PREMISE
        )
