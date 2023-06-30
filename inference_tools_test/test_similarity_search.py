import unittest

from inference_tools_test.data.dataclasses.knowledge_graph_forge_test import KnowledgeGraphForgeTest
from inference_tools.datatypes.query import query_factory
from inference_tools.execution import execute_query_object


class SimilaritySearchTest(unittest.TestCase):

    def setUp(self):
        self.forge_factory = lambda a, b: KnowledgeGraphForgeTest({"org": a, "project": b})

    def test_execute(self):
        similarity_search_query = {
            "@type": "SimilarityQuery",
            "hasParameter": [
                {
                    "@type": "uri",
                    "description": "test param",
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
                        "@id": "embedding_model_id",
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
