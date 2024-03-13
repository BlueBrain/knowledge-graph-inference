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

from inference_tools.datatypes.parameter_specification import ParameterSpecification
from inference_tools.exceptions.malformed_rule import InvalidParameterTypeException

from inference_tools.datatypes.query import query_factory, SparqlQueryBody

from inference_tools.multi_predicate_object_pair import (
    has_multi_predicate_object_pairs,
    multi_predicate_object_pairs_query_rewriting,
    multi_predicate_object_pairs_parameter_rewriting,
    multi_check
)

from inference_tools.utils import format_parameters


@pytest.fixture
def query_without(query_conf):
    return {
        "@type": "SparqlQuery",
        "hasBody": {"query_string": ""},
        "hasParameter": [],
        "queryConfiguration": query_conf,
        "resultParameterMapping": []
    }


@pytest.fixture
def query_with(query_conf):
    return {
        "type": "SparqlQuery",
        "hasBody": {"query_string":
            """
                SELECT ?id ?br
                WHERE { 
                    ?id $whatever .
                    ?id nsg:brainLocation/nsg:brainRegion ?br .
                    }
            """},
        "hasParameter": [
            {
                "type": "MultiPredicateObjectPair",
                "description": "paths to the properties being checked",
                "name": "whatever"
            }
        ],
        "queryConfiguration": query_conf,
        "resultParameterMapping": []
    }


@pytest.fixture
def existing_parameter_values():
    return {
        "whatever": [
            (
                ("rdf:type", "path"),
                ("<https://neuroshapes.org/NeuronMorphology>", "uri")
            ),
            (
                ("contribution/agent", "path"),
                (
                    "<https://bbp.epfl.ch/neurosciencegraph/data/7c47aa15-9fc6-42ec-9871-d233c9c29028>",
                    "uri"
                )
            )
        ]
    }


@pytest.fixture
def rewritten_query():
    e = """
                SELECT ?id ?br
                WHERE { 
                    ?id $whatever_0_predicate $whatever_0_object .
                    ?id $whatever_1_predicate $whatever_1_object .
                    ?id nsg:brainLocation/nsg:brainRegion ?br .
                    }
            """  # TODO super sensitive to tabs for exact string equally, change the test
    return SparqlQueryBody({"query_string": e})


@pytest.fixture
def expected_formatted_parameters():
    return {
        'whatever_0_predicate': 'rdf:type',
        'whatever_0_object': '<https://neuroshapes.org/NeuronMorphology>',
        'whatever_1_predicate': 'contribution/agent',
        'whatever_1_object':
            '<https://bbp.epfl.ch/neurosciencegraph/data/7c47aa15-9fc6-42ec-9871-d233c9c29028>'
    }


def test_has_multi(query_with, existing_parameter_values):
    query = query_factory(query_with)

    has_multi = has_multi_predicate_object_pairs(
        query.parameter_specifications, existing_parameter_values
    )

    idx, name, nb_multi = has_multi

    assert idx == 0  # First parameter specification
    assert name == "whatever"
    assert nb_multi == 2


def test_has_no_multi(query_without):
    query = query_factory(query_without)

    has_multi = has_multi_predicate_object_pairs(
        query.parameter_specifications, parameter_values={}
    )

    assert has_multi is None


def test_multi_no_parameter_value(query_with):
    # Since it's an extension of a query, it is by default optional

    query = query_factory(query_with)

    has_multi = has_multi_predicate_object_pairs(
        query.parameter_specifications, parameter_values={}
    )

    idx, name, nb_multi = has_multi

    assert idx == 0  # First parameter specification
    assert name == "whatever"
    assert nb_multi == 0


def test_parameter_format_multi_predicate(
        query_with, existing_parameter_values, forge, rewritten_query,
        expected_formatted_parameters
):
    query = query_factory(query_with)

    formatted_parameters = format_parameters(
        query=query, parameter_values=existing_parameter_values,
        forge=forge
    )

    assert query.body.query_string == rewritten_query.query_string
    assert formatted_parameters == expected_formatted_parameters


def test_multi_predicate_object_pairs_query_rewriting(
        query_with, existing_parameter_values, rewritten_query
):
    query = query_factory(query_with)

    idx, name, nb_multi = has_multi_predicate_object_pairs(
        query.parameter_specifications, parameter_values=existing_parameter_values
    )

    computed_rewritten_query = multi_predicate_object_pairs_query_rewriting(
        name=name, query_body=query.body, nb_multi=nb_multi
    )

    assert rewritten_query.query_string == computed_rewritten_query.query_string


def test_multi_predicate_object_pairs_parameter_rewriting(
        query_with, existing_parameter_values, expected_formatted_parameters
):
    query = query_factory(query_with)

    idx, name, nb_multi = has_multi_predicate_object_pairs(
        query.parameter_specifications, parameter_values=existing_parameter_values
    )

    parameter_spec, parameter_values = multi_predicate_object_pairs_parameter_rewriting(
        idx,
        query.parameter_specifications,
        parameter_values=existing_parameter_values
    )

    make_spec = lambda name, type: ParameterSpecification(obj={"name": name, "type": type})

    expected_parameter_spec = [
        make_spec(name="whatever_0_predicate", type="path"),
        make_spec(name="whatever_0_object", type="uri"),
        make_spec(name="whatever_1_predicate", type="path"),
        make_spec(name="whatever_1_object", type="uri")
    ]

    assert parameter_values == expected_formatted_parameters
    assert expected_parameter_spec == parameter_spec


def test_multi_wrong_query_type(query_with):
    query_with_wrong_type = query_with.copy()
    query_with_wrong_type["type"] = "ElasticSearchQuery"
    query = query_factory(query_with_wrong_type)

    with pytest.raises(InvalidParameterTypeException):
        multi_check(query, {})
