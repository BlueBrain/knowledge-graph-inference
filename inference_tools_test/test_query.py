import unittest

from inference_tools.datatypes.query import Query, query_factory, SparqlQuery, ElasticSearchQuery, \
    SimilaritySearchQuery, ForgeQuery
from inference_tools.exceptions.exceptions import InferenceToolsException, InvalidValueException, \
    IncompleteObjectException
from inference_tools.execution import execute_query_object
from inference_tools.nexus_utils.bucket_configuration import NexusBucketConfiguration
from inference_tools.source.source import DEFAULT_LIMIT
from inference_tools.type import ParameterType, QueryType
from inference_tools.utils import format_parameters
from kgforge_test import KnowledgeGraphForgeTest


class QueryTest(unittest.TestCase):

    def setUp(self):  # TODO setupclass?
        self.query_conf = {
            "org": "org_i",
            "project": "project_i",
        }
        self.forge_factory = lambda a, b: KnowledgeGraphForgeTest()


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

    def test_idk(self):
        similarity_search_query = {
            "@type": "SimilarityQuery",
            "hasParameter": [
                {
                    "@type": "uri",
                    "description": "test param ",
                    "name": "TargetResourceParameter"
                }
            ],
            "k": 50,
            "queryConfiguration": [
                {
                    "boosted": True,
                    "boostingView": {
                        "@id": "boosting_view_id",
                        "@type": "ElasticSearchView"
                    },
                    "description": "Model description",
                    "embeddingModel": {
                        "@id": "entity_id",
                        "@type": "EmbeddingModel",
                        "hasSelector": {
                            "@type": "FragmentSelector",
                            "conformsTo":
                                "https://bluebrainnexus.io/docs/delta/api/resources-api.html#fetch",
                            "value": "?rev=17"
                        },
                        "org": "org_i",
                        "project": "project_i"
                    },
                    "org": "org_i",
                    "project": "project_i",
                    "similarityView": {
                        "@id": "similarity_view_id",
                        "@type": "ElasticSearchView"
                    },
                    "statisticsView": {
                        "@id": "stat_view_id",
                        "@type": "ElasticSearchView"
                    }
                }
            ],
            "searchTargetParameter": "TargetResourceParameter"
        }

        execute_query_object(
            query=query_factory(similarity_search_query),
            forge_factory=self.forge_factory,
            parameter_values={"TargetResourceParameter": "any"}
        )

