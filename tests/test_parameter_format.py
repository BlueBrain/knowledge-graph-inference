import unittest

from inference_tools.datatypes.query import Query, query_factory
from inference_tools.exceptions.exceptions import InferenceToolsException
from inference_tools.nexus_utils.bucket_configuration import NexusBucketConfiguration
from inference_tools.source.source import DEFAULT_LIMIT
from inference_tools.type import ParameterType
from inference_tools.utils import format_parameters


class ParameterFormattingTest(unittest.TestCase):

    def setUp(self):  # TODO setupclass?
        self.query_conf = {
                "org": "bbp",
                "project": "atlas",
            }
        self.some_forge_object = NexusBucketConfiguration(
            self.query_conf["org"], self.query_conf["project"], True
        ).allocate_forge_session()

    def test_parameter_format_limit(self):
        q0 = {
            "@type": "SparqlQuery",
            "hasBody": "",
            "hasParameter": [],
            "queryConfiguration": self.query_conf,
            "resultParameterMapping": []
        }

        query: Query = query_factory(q0)
        parameter_values = {}

        limit, formatted_parameters = format_parameters(
            query=query, parameter_values=parameter_values, forge=self.some_forge_object
        )

        self.assertEqual(limit, DEFAULT_LIMIT)

        limit_value = 50
        parameter_values["LimitQueryParameter"] = limit_value

        limit2, formatted_parameters2 = format_parameters(
            query=query, parameter_values=parameter_values, forge=self.some_forge_object
        )

        self.assertEqual(limit2, limit_value)

    def test_parameter_format_missing_mandatory(self):

        field_name = "MandatoryField"
        q1 = {
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
            "queryConfiguration": self.query_conf,
            "resultParameterMapping": []
        }

        query: Query = query_factory(q1)

        parameter_values = {}

        with self.assertRaises(InferenceToolsException):
            _, _ = format_parameters(
                query=query, parameter_values=parameter_values, forge=self.some_forge_object
            )

        parameter_values[field_name] = ["a", "b"]
        _, formatted_parameters = format_parameters(
            query=query, parameter_values=parameter_values, forge=self.some_forge_object
        )

        self.assertIsInstance(formatted_parameters, dict)
        self.assertNotEqual(len(formatted_parameters), 0)

    def test_parameter_format_list_formatting(self):

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
            q1 = {
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
                "queryConfiguration": self.query_conf,
                "resultParameterMapping": []
            }

            _, formatted_parameters = format_parameters(
                query=query_factory(q1), parameter_values=parameter_values,
                forge=self.some_forge_object
            )

            self.assertDictEqual(formatted_parameters, {field_name: expected_value})
