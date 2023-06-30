import unittest

from inference_tools.execution import apply_rule
from inference_tools_test.data.dataclasses.knowledge_graph_forge_test import KnowledgeGraphForgeTest


class InferenceTest(unittest.TestCase):

    def setUp(self):  # TODO setupclass?
        self.query_conf = {
            "org": "bbp",
            "project": "atlas",
        }

        self.forge_factory = lambda a, b: KnowledgeGraphForgeTest()

    def test_infer(self):
        q = {
            "@type": "SparqlQuery",
            "hasBody": "",
            "hasParameter": [],
            "queryConfiguration": self.query_conf,
            "resultParameterMapping": []
        }

        rule_dict = {
          "@id": "test",
          "@type": "DataGeneralizationRule",
          "description": "Test Rule description",
          "name": "Test rule",
          "searchQuery": q,
          "targetResourceType": "Entity"
        }

        test = apply_rule(forge_factory=self.forge_factory, parameter_values={}, rule=rule_dict)




