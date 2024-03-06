import json
from typing import Dict, Optional, List, Union, Any

from kgforge.core import KnowledgeGraphForge, Resource

from inference_tools.datatypes.query import ElasticSearchQuery
from inference_tools.datatypes.query_configuration import ElasticSearchQueryConfiguration
from inference_tools.premise_execution import PremiseExecution
from inference_tools.source.source import Source, DEFAULT_LIMIT


class ElasticSearch(Source):
    NO_LIMIT = 10000

    @staticmethod
    def execute_query(
            forge: KnowledgeGraphForge,
            query: ElasticSearchQuery,
            parameter_values: Dict,
            config: ElasticSearchQueryConfiguration,
            limit=DEFAULT_LIMIT,
            debug: bool = False
    ) -> Optional[List[Dict]]:

        query_body = json.dumps(query.body)

        for k, v in parameter_values.items():
            query_body = query_body.replace(f"\"${k}\"", str(v))

        return forge.elastic(query_body, limit=limit, debug=debug, as_resource=False)

    @staticmethod
    def check_premise(
            forge: KnowledgeGraphForge, premise: ElasticSearchQuery,
            parameter_values: Dict,
            config: ElasticSearchQueryConfiguration, debug: bool = False
    ):

        results = ElasticSearch.execute_query(
            forge=forge, query=premise,
            parameter_values=parameter_values,
            debug=debug, config=config, limit=None
        )

        return PremiseExecution.SUCCESS if results is not None and len(results) > 0 else \
            PremiseExecution.FAIL

    @staticmethod
    def get_all_documents_query():
        return {
            "size": ElasticSearch.NO_LIMIT,
            "query": {
                "term": {
                    "_deprecated": False
                }
            }
        }

    @staticmethod
    def get_all_documents(forge: KnowledgeGraphForge) -> Optional[List[Resource]]:
        """
        Retrieves all Resources that are indexed by the current elastic view endpoint of the forge
        instance
        @param forge: the forge instance
        @type forge: KnowledgeGraphForge
        @return:
        @rtype:  Optional[List[Resource]]
        """
        return forge.elastic(json.dumps(ElasticSearch.get_all_documents_query()))

    @staticmethod
    def get_by_id(ids: Union[str, List[str]], forge: KnowledgeGraphForge) -> \
            Optional[Union[Resource, List[Resource]]]:
        """

        @param ids: the list of ids of the resources to retrieve
        @type ids: List[str]
        @param forge: a forge instance
        @type forge: KnowledgeGraphForge
        @return: the list of Resources retrieved, if successful else None
        @rtype: Optional[List[Resource]]
        """
        q: Dict[str, Any] = {
            "size": ElasticSearch.NO_LIMIT,
            'query': {
                'bool': {
                    'filter': [
                        {'terms': {'@id': ids}} if isinstance(ids, list) else {'term': {'@id': ids}}
                    ],
                    'must': [
                        {'match': {'_deprecated': False}}
                    ]
                }
            }
        }
        res = forge.elastic(json.dumps(q), debug=False)
        return res[0] if isinstance(ids, str) and len(res) == 1 else res
