import unittest

from inference_tools.datatypes.rule import Rule

from inference_tools.utils import get_search_query_parameters


class GetRuleParametersTest(unittest.TestCase):

    def setUp(self):
        self.query_conf = {
            "org": "bbp",
            "project": "atlas",
        }

    def test_get_search_query_parameters(self):
        rule_dict = {
            "@id": "id_value",
            "@type": "DataGeneralizationRule",
            "description": "Test rule desc",
            "name": "Test rule",
            "searchQuery": {
                "@type": "QueryPipe",
                "head": {
                    "@type": "SparqlQuery",
                    "hasBody": "",
                    "hasParameter": [
                        {"@type": "path", "name": "param1"},
                        {"@type": "sparql_list", "name": "param2"},
                        {"@type": "path", "name": "param3"},
                    ],
                    "queryConfiguration": self.query_conf,
                    "resultParameterMapping": [
                        {"parameterName": "param4", "path": "id"}
                    ]
                },
                "rest": {
                    "@type": "QueryPipe",
                    "head": {
                        "@type": "SparqlQuery",
                        "hasBody": "",
                        "hasParameter": [
                            {"@type": "path", "name": "param4"},
                            {"@type": "sparql_list", "name": "param5"},
                            {"@type": "path", "name": "param6"},
                        ],
                        "queryConfiguration": self.query_conf,
                        "resultParameterMapping": [
                            {"parameterName": "param7", "path": "id"}
                        ]
                    },
                    "rest": {
                        "@type": "SparqlQuery",
                        "hasBody": "",
                        "hasParameter": [
                            {"@type": "sparql_list", "name": "param7"},
                            {"@type": "path", "name": "param8"},
                            {"@type": "MultiPredicateObjectPair", "name": "param9"},
                            {"@type": "path", "name": "param10"}
                        ],
                        "queryConfiguration": self.query_conf
                    }
                }
            },
            "targetResourceType": "Entity"
        }

        rule = Rule(rule_dict)

        self.assertEqual(
            [
                f"param{i}" for i in range(1, 11)
                if i not in [4, 7]
            ],
            list(get_search_query_parameters(rule).keys()),
        )

        rule.search_query.head.result_parameter_mapping = []

        self.assertEqual(
            [
                f"param{i}" for i in range(1, 11)
                if i != 7
            ],
            list(get_search_query_parameters(rule).keys()),
        )

        rule.search_query.rest.head.result_parameter_mapping = []

        self.assertEqual(
            [f"param{i}" for i in range(1, 11)],
            list(get_search_query_parameters(rule).keys()),
        )
