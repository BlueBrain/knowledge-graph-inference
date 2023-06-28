import unittest

from tests.kgforge_test import KnowledgeGraphForgeTest


class InferenceTest(unittest.TestCase):

    def setUp(self):  # TODO setupclass?
        self.query_conf = {
            "org": "bbp",
            "project": "atlas",
        }
        self.forge_factory = lambda a, b: KnowledgeGraphForgeTest()

    def test_infer(self):


