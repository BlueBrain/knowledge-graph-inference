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

import json
from string import Template
from typing import Dict, Optional, List

from kgforge.core import KnowledgeGraphForge, Resource

from inference_tools.datatypes.query_configuration import ForgeQueryConfiguration
from inference_tools.exceptions.malformed_rule import MalformedRuleException
from inference_tools.datatypes.query import ForgeQuery
from inference_tools.helper_functions import _enforce_list, _follow_path, get_id_attribute
from inference_tools.premise_execution import PremiseExecution
from inference_tools.exceptions.exceptions import InferenceToolsException
from inference_tools.source.source import Source, DEFAULT_LIMIT


class Forge(Source):

    @staticmethod
    def execute_query(
            forge: KnowledgeGraphForge,
            query: ForgeQuery,
            parameter_values: Dict,
            config: ForgeQueryConfiguration,
            limit: Optional[int] = DEFAULT_LIMIT,
            debug: bool = False
    ) -> Optional[List[Resource]]:
        """
        Executes a forge.search query by turning it into a sparql query,
        within the bucket defined by the config,
        and targeting the sparql view defined by the config

        @param forge: a forge instance
        @type forge: KnowledgeGraphForge
        @param query: the query to execute
        @type query: ForgeQuery
        @param parameter_values: the parameters to use inside the parametrisable query
        @type parameter_values: Dict
        @param config: the query configuration, holding the bucket to target and
        the sparql view within it
        @type config: ForgeQueryConfiguration
        @param limit: the maximum number of results to get from the execution
        @type limit: int
        @param debug: Whether to print out the query before its execution
        @type debug: bool
        @return: the results of the query execution
        @rtype: List[Dict]
        """

        q = json.loads(
            Template(json.dumps(query.body)).substitute(**parameter_values)
        )

        return forge.search(q, debug=debug, limit=limit)

    @staticmethod
    def check_premise(
            forge: KnowledgeGraphForge, premise: ForgeQuery,
            parameter_values: Dict, config: ForgeQueryConfiguration, debug: bool = False
    ) -> PremiseExecution:
        """
        Executes a premise based on a forge.search query.
        @param forge: a forge instance
        @type forge: KnowledgeGraphForge
        @param premise: the premise holding the query to execute
        @type premise: ForgeQuery
        @param parameter_values: the parameters to use inside the parametrisable query of the premise
        @type parameter_values: Dict
        @param config: the query configuration, holding the bucket to target and the view within it
        @type config: ForgeQueryConfiguration
        @param debug: Whether to print out the premise's query before its execution
        @type debug: bool
        @return: PremiseExecution.FAIL is running the query within it has returned no results,
        PremiseExecution.SUCCESS otherwise.
        @rtype: PremiseExecution
        """

        resources = Forge.execute_query(
            forge=forge, query=premise,
            parameter_values=parameter_values, config=config,
            debug=debug, limit=None
        )

        if resources is None:
            return PremiseExecution.FAIL

        resources_list: List[Resource] = _enforce_list(forge.as_json(resources))

        if premise.target_parameter:
            if premise.target_path:
                try:
                    matched_values = [
                        _follow_path(r, premise.target_path)
                        for r in resources_list
                    ]
                except InferenceToolsException:
                    return PremiseExecution.FAIL
            else:
                matched_values = [get_id_attribute(r) for r in resources_list]

            if parameter_values[premise.target_parameter] not in matched_values:
                return PremiseExecution.FAIL
        else:
            if len(resources_list) == 0:
                return PremiseExecution.FAIL

            return PremiseExecution.SUCCESS

        raise MalformedRuleException("Missing target parameter in Forge Search Premise")
