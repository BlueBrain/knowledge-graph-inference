import unittest

from inference_tools.datatypes.query import Query, query_factory
from inference_tools.exceptions.exceptions import InferenceToolsException
from inference_tools.nexus_utils.bucket_configuration import NexusBucketConfiguration
from inference_tools.source.source import DEFAULT_LIMIT
from inference_tools.utils import format_parameters


class ParameterFormattingTest(unittest.TestCase):

    def setUp(self):
        self.query_conf = {
                "org": "bbp",
                "project": "atlas",
            }
        self.some_forge_object =  NexusBucketConfiguration(
            self.query_conf["org"], self.query_conf["project"], True
        ).allocate_forge_session()

    def test_parameter_format_limit(self):
        q0 = {
            "@type": "SparqlQuery",
            "hasBody": "",
            "hasParameter": [],
            "queryConfiguration": self.query_conf ,
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
        q1 = {
            "@type": "SparqlQuery",
            "hasBody": "",
            "hasParameter": [
                {
                    "@type": "sparql_list",
                    "description": "test field",
                    "name": "MandatoryField",
                    "optional": False
                }
            ],
            "queryConfiguration": self.query_conf,
            "resultParameterMapping": []
        }

        query: Query = query_factory(q1)

        parameter_values = {}

        with self.assertRaises(InferenceToolsException):
            limit, formatted_parameters = format_parameters(
                query=query, parameter_values=parameter_values, forge=self.some_forge_object
            )

