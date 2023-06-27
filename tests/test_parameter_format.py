import unittest

from inference_tools.datatypes.query import Query, query_factory
from inference_tools.exceptions.exceptions import InferenceToolsException
from inference_tools.nexus_utils.bucket_configuration import NexusBucketConfiguration
from inference_tools.source.source import DEFAULT_LIMIT
from inference_tools.utils import format_parameters


class ParameterFormattingTest(unittest.TestCase):

    def test_parameter_format_limit(self):
        q0 = {
            "@type": "SparqlQuery",
            "hasBody": "",
            "hasParameter": [],
            "queryConfiguration": {
                "org": "bbp",
                "project": "atlas",
            },
            "resultParameterMapping": []
        }

        query: Query = query_factory(q0)

        query_config_0 = query.query_configurations[0]
        forge = NexusBucketConfiguration(query_config_0.org, query_config_0.project,
                                         True).allocate_forge_session()

        parameter_values = {}
        limit, formatted_parameters = format_parameters(
            query=query, parameter_values=parameter_values, forge=forge
        )

        self.assertEqual(limit, DEFAULT_LIMIT)

        limit_value = 50
        parameter_values["LimitQueryParameter"] = limit_value

        limit2, formatted_parameters2 = format_parameters(
            query=query, parameter_values=parameter_values, forge=forge
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
            "queryConfiguration": {
                "org": "bbp",
                "project": "atlas"
            },
            "resultParameterMapping": []
        }

        query: Query = query_factory(q1)
        query_config_0 = query.query_configurations[0]
        forge = NexusBucketConfiguration(
            query_config_0.org, query_config_0.project, True
        ).allocate_forge_session()

        parameter_values = {}

        with self.assertRaises(InferenceToolsException):
            limit, formatted_parameters = format_parameters(
                query=query, parameter_values=parameter_values, forge=forge
            )

