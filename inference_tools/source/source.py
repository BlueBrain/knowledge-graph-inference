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

from abc import ABC, abstractmethod
from typing import Dict

from kgforge.core import KnowledgeGraphForge

from inference_tools.premise_execution import PremiseExecution

DEFAULT_LIMIT = 20


class Source(ABC):

    @staticmethod
    @abstractmethod
    def execute_query(
            forge: KnowledgeGraphForge,
            query,
            parameter_values: Dict,
            config,
            limit=DEFAULT_LIMIT,
            debug: bool = False
    ):
        """
        Executes a query
        @param forge: a forge instance
        @type forge: KnowledgeGraphForge
        @param query: the query to execute
        @type query:
        @param parameter_values: the parameters to use inside the parametrisable query
        @type parameter_values: Dict
        @param config: the query configuration, holding the bucket to target and the view within it
        @type config:
        @param limit: the maximum number of results to get from the execution
        @type limit: int
        @param debug: Whether to print out the query before its execution
        @type debug: bool
        @return: the results of the query execution
        @rtype:
        """

    @staticmethod
    @abstractmethod
    def check_premise(
            forge: KnowledgeGraphForge,
            premise,
            parameter_values: Dict,
            config,
            debug: bool = False
    ) -> PremiseExecution:
        """
        Executes a premise.
        @param forge: a forge instance
        @type forge: KnowledgeGraphForge
        @param premise: the premise holding the query to execute
        @type premise:
        @param parameter_values: the parameters to use inside the parametrisable query of the premise
        @type parameter_values: Dict
        @param config: the query configuration, holding the bucket to target and the view within it
        @type config:
        @param debug: Whether to print out the premise's query before its execution
        @type debug: bool
        @return: PremiseExecution.FAIL is running the query within it has returned no results,
        PremiseExecution.SUCCESS otherwise.
        @rtype: PremiseExecution
        """
