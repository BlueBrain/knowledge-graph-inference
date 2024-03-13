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
from typing import Dict, Optional, List

from string import Template

from kgforge.core import KnowledgeGraphForge, Resource

from inference_tools.type import ParameterType
from inference_tools.datatypes.query import SparqlQuery
from inference_tools.datatypes.query_configuration import SparqlQueryConfiguration
from inference_tools.premise_execution import PremiseExecution
from inference_tools.source.source import Source, DEFAULT_LIMIT


class Sparql(Source):
    @staticmethod
    def execute_query(
            forge: KnowledgeGraphForge,
            query: SparqlQuery,
            parameter_values: Dict,
            config: SparqlQueryConfiguration,
            limit: Optional[int] = DEFAULT_LIMIT,
            debug: bool = False
    ) -> List[Resource]:
        """
        Executes a sparql query
        @param forge: a forge instance
        @type forge: KnowledgeGraphForge
        @param query: the query to execute
        @type query: SparqlQuery
        @param parameter_values: the parameters to use inside the parametrisable query
        @type parameter_values: Dict
        @param config: the query configuration, holding the bucket to target and
        the sparql view within it
        @type config: SparqlQueryConfiguration
        @param limit: the maximum number of results to get from the execution
        @type limit: int
        @param debug: Whether to print out the query before its execution
        @type debug: bool
        @return: the results of the query execution
        @rtype: List[Dict]
        """

        query_body = query.body.query_string

        query_blocks = [x for x in query.parameter_specifications
                        if x.type == ParameterType.QUERY_BLOCK]

        if len(query_blocks) != 0:
            for qb in query_blocks:
                to_replace = f"${qb.name}"
                query_body = query_body.replace(to_replace, parameter_values[qb.name])

        query_body = Template(query_body).substitute(**parameter_values)

        return forge.sparql(query_body, limit=limit, debug=debug)

    @staticmethod
    def check_premise(
            forge: KnowledgeGraphForge, premise: SparqlQuery, parameter_values: Dict,
            config: SparqlQueryConfiguration, debug: bool = False
    ) -> PremiseExecution:
        """
        Executes a premise based on a sparql query.
        @param forge: a forge instance
        @type forge: KnowledgeGraphForge
        @param premise: the premise holding the query to execute
        @type premise: SparqlQuery
        @param parameter_values: the parameters to use inside the parametrisable query of the premise
        @type parameter_values: Dict
        @param config: the query configuration, holding the bucket to target and the view within it
        @type config: SparqlQueryConfiguration
        @param debug: Whether to print out the premise's query before its execution
        @type debug: bool
        @return: PremiseExecution.FAIL is running the query within it has returned no results,
        PremiseExecution.SUCCESS otherwise.
        @rtype: PremiseExecution
        """

        results = Sparql.execute_query(
            forge=forge, query=premise,
            parameter_values=parameter_values,
            debug=debug, config=config, limit=None
        )

        return PremiseExecution.SUCCESS if results is not None and len(results) > 0 else \
            PremiseExecution.FAIL
