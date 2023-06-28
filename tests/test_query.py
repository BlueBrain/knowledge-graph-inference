import unittest

from inference_tools.datatypes.query import Query, query_factory
from inference_tools.exceptions.exceptions import InferenceToolsException, InvalidValueException
from inference_tools.nexus_utils.bucket_configuration import NexusBucketConfiguration
from inference_tools.source.source import DEFAULT_LIMIT
from inference_tools.type import ParameterType
from inference_tools.utils import format_parameters


class QueryFormatTest(unittest.TestCase):

    def setUp(self):  # TODO setupclass?
        self.query_conf = {
                "org": "bbp",
                "project": "atlas",
            }
        # self.some_forge_object = NexusBucketConfiguration(
        #     self.query_conf["org"], self.query_conf["project"], True
        # ).allocate_forge_session()

    def test_query_type(self):

        types = ["SparqlQuery", "ForgeSearchQuery", "SimilarityQuery", "ElasticSearchQuery"]

        query_maker = lambda type_: query_factory({
                "type": type_,
                "hasBody": "",
                "hasParameter": [],
                "queryConfiguration": self.query_conf,
                "resultParameterMapping": []
            })

        for type_ in types:
            query = query_maker(type_)

        with self.assertRaises(InvalidValueException):
            query = query_maker("InvalidType")





