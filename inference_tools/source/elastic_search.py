# This file is part of knowledge-graph-inference.
# Copyright 2024 Blue Brain Project / EPFL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# pylint: disable=R0801
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
            limit: Optional[int] = DEFAULT_LIMIT,
            debug: bool = False
    ) -> List[Dict]:
        """
        Executes an elastic search query
        @param forge: a forge instance
        @type forge: KnowledgeGraphForge
        @param query: the query to execute
        @type query: ElasticSearchQuery
        @param parameter_values: the parameters to use inside the parametrisable query
        @type parameter_values: Dict
        @param config: the query configuration, holding the bucket to target and
        the elastic search view within it
        @type config: ElasticSearchQueryConfiguration
        @param limit: the maximum number of results to get from the execution
        @type limit: int
        @param debug: Whether to print out the query before its execution
        @type debug: bool
        @return: the results of the query execution
        @rtype: List[Dict]
        """
        query_body = json.dumps(query.body)

        for k, v in parameter_values.items():
            query_body = query_body.replace(f"\"${k}\"", str(v))

        return forge.elastic(query_body, limit=limit, debug=debug, as_resource=False)

    @staticmethod
    def check_premise(
            forge: KnowledgeGraphForge,
            premise: ElasticSearchQuery,
            parameter_values: Dict,
            config: ElasticSearchQueryConfiguration,
            debug: bool = False
    ) -> PremiseExecution:
        """
        Executes a premise based on an elastic search query.
        @param forge: a forge instance
        @type forge: KnowledgeGraphForge
        @param premise: the premise holding the query to execute
        @type premise: ElasticSearchQuery
        @param parameter_values: the parameters to use inside the parametrisable query of the premise
        @type parameter_values: Dict
        @param config: the query configuration, holding the bucket to target and the view within it
        @type config: ElasticSearchQueryConfiguration
        @param debug: Whether to print out the premise's query before its execution
        @type debug: bool
        @return: PremiseExecution.FAIL is running the query within it has returned no results,
        PremiseExecution.SUCCESS otherwise.
        @rtype: PremiseExecution
        """
        results = ElasticSearch.execute_query(
            forge=forge, query=premise,
            parameter_values=parameter_values,
            debug=debug, config=config, limit=None
        )

        return PremiseExecution.SUCCESS if results is not None and len(results) > 0 else \
            PremiseExecution.FAIL

    @staticmethod
    def get_all_documents_query() -> Dict:
        """
        Return a query which would return all documents that are not deprecated in an ES index.
        A hardcoded limit of 10000 is set, so that forge doesn't add its default limit.
        @return: the ES query as a dictionary
        @rtype: Dict
        """
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
        Get a document by id from the elastic search index associated to the
        forge instance's elastic search view
        @param ids: the list of ids of the resources to retrieve
        @type ids: List[str]
        @param forge: a forge instance, holding the elastic search view to target
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
