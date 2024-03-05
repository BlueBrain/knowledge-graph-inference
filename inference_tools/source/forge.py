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
            forge: KnowledgeGraphForge, query: ForgeQuery,
            parameter_values: Dict, config: ForgeQueryConfiguration,
            limit=DEFAULT_LIMIT, debug: bool = False
    ) -> Optional[List[Resource]]:

        q = json.loads(
            Template(json.dumps(query.body)).substitute(**parameter_values)
        )

        return forge.search(q, debug=debug, limit=limit)

    @staticmethod
    def check_premise(
            forge: KnowledgeGraphForge, premise: ForgeQuery,
            parameter_values: Dict, config: ForgeQueryConfiguration, debug: bool = False):

        resources = Forge.execute_query(forge=forge, query=premise,
                                        parameter_values=parameter_values, config=config,
                                        debug=debug, limit=None)

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
