import pytest

from inference_tools_test.data.classes.knowledge_graph_forge_test import KnowledgeGraphForgeTest
from inference_tools.datatypes.query import Query, query_factory
from inference_tools.exceptions.exceptions import InferenceToolsException, InvalidValueException

from inference_tools.source.source import DEFAULT_LIMIT
from inference_tools.type import ParameterType
from inference_tools.utils import format_parameters


@pytest.fixture
def query_conf():
    return {
        "org": "bbp",
        "project": "atlas",
    }


@pytest.fixture
def some_forge_object(query_conf):
    return KnowledgeGraphForgeTest(query_conf)


def test_parameter_format_limit(query_conf, some_forge_object):
    q = {
        "@type": "SparqlQuery",
        "hasBody": "",
        "hasParameter": [],
        "queryConfiguration": query_conf,
        "resultParameterMapping": []
    }

    query: Query = query_factory(q)
    parameter_values = {}

    limit, formatted_parameters = format_parameters(
        query=query, parameter_values=parameter_values, forge=some_forge_object
    )

    assert limit == DEFAULT_LIMIT

    limit_value = 50
    parameter_values["LimitQueryParameter"] = limit_value

    limit2, formatted_parameters2 = format_parameters(
        query=query, parameter_values=parameter_values, forge=some_forge_object
    )

    assert limit2 == limit_value


def test_parameter_format_missing_mandatory(query_conf, some_forge_object):
    field_name = "MandatoryField"
    q = {
        "@type": "SparqlQuery",
        "hasBody": "",
        "hasParameter": [
            {
                "@type": "sparql_list",
                "description": "test field",
                "name": field_name,
                "optional": False
            }
        ],
        "queryConfiguration": query_conf,
        "resultParameterMapping": []
    }

    query: Query = query_factory(q)

    parameter_values = {}

    with pytest.raises(InferenceToolsException):
        _, _ = format_parameters(
            query=query, parameter_values=parameter_values, forge=some_forge_object
        )

    parameter_values[field_name] = ["a", "b"]
    _, formatted_parameters = format_parameters(
        query=query, parameter_values=parameter_values, forge=some_forge_object
    )

    assert isinstance(formatted_parameters, dict)
    assert len(formatted_parameters) != 0


def test_parameter_format_list_formatting(query_conf, some_forge_object):
    field_name = "ListField"
    parameter_values = {field_name: ["a", "b"]}

    expected_values = {
        ParameterType.SPARQL_LIST.value: '(<a>, <b>)',
        ParameterType.LIST.value: '"a", "b"',
        ParameterType.SPARQL_VALUE_LIST.value: '("a")\n("b")',
        ParameterType.SPARQL_VALUE_URI_LIST.value:
            "(<https://bbp.epfl.ch/nexus/v1/resources/bbp/atlas/_/a>)\n"
            "(<https://bbp.epfl.ch/nexus/v1/resources/bbp/atlas/_/b>)",
        ParameterType.URI_LIST.value:
            "<https://bbp.epfl.ch/nexus/v1/resources/bbp/atlas/_/a>, "
            "<https://bbp.epfl.ch/nexus/v1/resources/bbp/atlas/_/b>",
    }

    for field_type, expected_value in expected_values.items():
        q = {
            "@type": "SparqlQuery",
            "hasBody": "",
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

        _, formatted_parameters = format_parameters(
            query=query_factory(q), parameter_values=parameter_values,
            forge=some_forge_object
        )

        assert formatted_parameters == {field_name: expected_value}


def test_parameter_format_value_formatting(query_conf, some_forge_object):
    field_name = "ValueField"

    def run_formatting(field_type, parameter_values):
        q = {
            "@type": "SparqlQuery",
            "hasBody": "",
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

        _, params = format_parameters(
            query=query_factory(q), parameter_values=parameter_values,
            forge=some_forge_object
        )

        return params

    # MULTI_PREDICATE_OBJECT_PAIR = "MultiPredicateObjectPair"
    # QUERY_BLOCK = "query_block"

    type_input_expected = [
        (
            ParameterType.URI.value, {field_name: "a"},
            {field_name: 'https://bbp.epfl.ch/nexus/v1/resources/bbp/atlas/_/a'}
        ),
        (ParameterType.STR.value, {field_name: "a"}, {field_name: '"a"'}),
        (ParameterType.PATH.value, {field_name: "a"}, {field_name: 'a'}),
        (ParameterType.BOOL.value, {field_name: "true"}, {field_name: "true"}),
        (ParameterType.BOOL.value, {field_name: "false"}, {field_name: "false"}),
        (ParameterType.BOOL.value, {field_name: "True"}, {field_name: "true"}),
        (ParameterType.BOOL.value, {field_name: "False"}, {field_name: "false"})
    ]

    for type_, values, expected_value in type_input_expected:
        formatted_parameters = run_formatting(type_, values)
        assert formatted_parameters == expected_value

    with pytest.raises(InvalidValueException):
        run_formatting(ParameterType.BOOL.value, {field_name: "idk"})
