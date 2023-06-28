import unittest

from inference_tools.datatypes.query import Query, query_factory, SparqlQuery, ElasticSearchQuery, \
    SimilaritySearchQuery, ForgeQuery
from inference_tools.exceptions.exceptions import InferenceToolsException, InvalidValueException, \
    IncompleteObjectException
from inference_tools.nexus_utils.bucket_configuration import NexusBucketConfiguration
from inference_tools.source.source import DEFAULT_LIMIT
from inference_tools.type import ParameterType, QueryType
from inference_tools.utils import format_parameters


class QueryTest(unittest.TestCase):

    def setUp(self):  # TODO setupclass?
        self.query_conf = {
            "org": "bbp",
            "project": "atlas",
        }

    def test_query_type(self):

        types = {
            QueryType.SPARQL_QUERY.value: SparqlQuery,
            QueryType.FORGE_SEARCH_QUERY: ForgeQuery,
            QueryType.SIMILARITY_QUERY: SimilaritySearchQuery,
            QueryType.ELASTIC_SEARCH_QUERY: ElasticSearchQuery
        }

        query_maker = lambda type_: query_factory({
            "type": type_,
            "queryConfiguration": self.query_conf
        })

        for type_, class_ in types.items():
            query = query_maker(type_)
            self.assertIsInstance(query, class_)

        with self.assertRaises(InvalidValueException):
            query_maker("InvalidType")

    def test_missing_query_configuration(self):

        types = ["SparqlQuery", "ForgeSearchQuery", "SimilarityQuery", "ElasticSearchQuery"]

        query_maker = lambda type_: query_factory({
            "type": type_
        })

        expected_error_msg = "The query  has been created with missing mandatory " \
                             "information: queryConfiguration"

        for type_ in types:
            with self.assertRaises(IncompleteObjectException, msg=expected_error_msg):
                query_maker(type_)

    def test_missing_query_has_body(self):
        pass
        # TODO current implementation doesn't fail if queries do not have a hasBody.
        #  Similarity, Elastic and Sparql should fail if no hasBody is provided.
        #  Forge shouldn't tho
