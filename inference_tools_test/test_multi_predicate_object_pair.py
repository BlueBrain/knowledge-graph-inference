import unittest

from inference_tools.datatypes.parameter_specification import ParameterSpecification
from inference_tools.exceptions.malformed_rule import InvalidParameterTypeException

from inference_tools.datatypes.query import query_factory

from inference_tools.multi_predicate_object_pair import (
    has_multi_predicate_object_pairs,
    multi_predicate_object_pairs_query_rewriting,
    multi_predicate_object_pairs_parameter_rewriting,
    multi_check
)

from inference_tools.nexus_utils.bucket_configuration import NexusBucketConfiguration
from inference_tools.utils import format_parameters


class MultiPredicateObjectPairTest(unittest.TestCase):

    def setUp(self):  # TODO setupclass?
        self.query_conf = {
            "org": "bbp",
            "project": "atlas",
        }
        self.some_forge_object = NexusBucketConfiguration(
            self.query_conf["org"], self.query_conf["project"], True
        ).allocate_forge_session()

        self.query_without = {
            "@type": "SparqlQuery",
            "hasBody": "",
            "hasParameter": [],
            "queryConfiguration": self.query_conf,
            "resultParameterMapping": []
        }

        self.query_with = {
            "type": "SparqlQuery",
            "hasBody":
                """
                    SELECT ?id ?br
                    WHERE { 
                        ?id $whatever .
                        ?id nsg:brainLocation/nsg:brainRegion ?br .
                        }
                """,
            "hasParameter": [
                {
                    "type": "MultiPredicateObjectPair",
                    "description": "paths to the properties being checked",
                    "name": "whatever"
                }
            ],
            "queryConfiguration": self.query_conf,
            "resultParameterMapping": []
        }

        self.existing_parameter_values = {
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

        self.rewritten_query = """
                    SELECT ?id ?br
                    WHERE { 
                        ?id $whatever_0_predicate $whatever_0_object .
                        ?id $whatever_1_predicate $whatever_1_object .
                        ?id nsg:brainLocation/nsg:brainRegion ?br .
                        }
                """  # TODO super sensitive to tabs for exact string equally, change the test

        self.expected_formatted_parameters = {
            'whatever_0_predicate': 'rdf:type',
            'whatever_0_object': '<https://neuroshapes.org/NeuronMorphology>',
            'whatever_1_predicate': 'contribution/agent',
            'whatever_1_object':
                '<https://bbp.epfl.ch/neurosciencegraph/data/7c47aa15-9fc6-42ec-9871-d233c9c29028>'
        }

    def test_has_multi(self):
        query = query_factory(self.query_with)

        has_multi = has_multi_predicate_object_pairs(
            query.parameter_specifications, self.existing_parameter_values
        )

        idx, name, nb_multi = has_multi

        self.assertEqual(idx, 0)  # First parameter specification
        self.assertEqual(name, "whatever")
        self.assertEqual(nb_multi, 2)

    def test_has_no_multi(self):
        query = query_factory(self.query_without)

        has_multi = has_multi_predicate_object_pairs(
            query.parameter_specifications, parameter_values={}
        )

        self.assertIsNone(has_multi)

    def test_multi_no_parameter_value(self):
        # Since it's an extension of a query, it is by default optional

        query = query_factory(self.query_with)

        has_multi = has_multi_predicate_object_pairs(
            query.parameter_specifications, parameter_values={}
        )

        idx, name, nb_multi = has_multi

        self.assertEqual(idx, 0)  # First parameter specification
        self.assertEqual(name, "whatever")
        self.assertEqual(nb_multi, 0)

    def test_parameter_format_multi_predicate(self):

        query = query_factory(self.query_with)

        _, formatted_parameters = format_parameters(
            query=query, parameter_values=self.existing_parameter_values,
            forge=self.some_forge_object
        )

        self.assertEqual(query.body, self.rewritten_query)
        self.assertDictEqual(formatted_parameters, self.expected_formatted_parameters)

    def test_multi_predicate_object_pairs_query_rewriting(self):
        query = query_factory(self.query_with)

        idx, name, nb_multi = has_multi_predicate_object_pairs(
            query.parameter_specifications, parameter_values=self.existing_parameter_values
        )

        rewritten_query = multi_predicate_object_pairs_query_rewriting(
            name=name, query_body=query.body, nb_multi=nb_multi
        )
        self.assertEqual(rewritten_query, self.rewritten_query)

    def test_multi_predicate_object_pairs_parameter_rewriting(self):
        query = query_factory(self.query_with)

        idx, name, nb_multi = has_multi_predicate_object_pairs(
            query.parameter_specifications, parameter_values=self.existing_parameter_values
        )

        parameter_spec, parameter_values = multi_predicate_object_pairs_parameter_rewriting(
            idx,
            query.parameter_specifications,
            parameter_values=self.existing_parameter_values
        )

        make_spec = lambda name, type: ParameterSpecification(obj={"name": name, "type": type})

        expected_parameter_spec = [
            make_spec(name="whatever_0_predicate", type="path"),
            make_spec(name="whatever_0_object", type="uri"),
            make_spec(name="whatever_1_predicate", type="path"),
            make_spec(name="whatever_1_object", type="uri")
        ]

        self.assertDictEqual(parameter_values, self.expected_formatted_parameters)
        self.assertEqual(expected_parameter_spec, parameter_spec)

    def test_multi_wrong_query_type(self):
        query_with_wrong_type = self.query_with.copy()
        query_with_wrong_type["type"] = "ElasticSearchQuery"
        query = query_factory(query_with_wrong_type)

        with self.assertRaises(InvalidParameterTypeException):
            multi_check(query, {})

