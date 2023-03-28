import json
from string import Template
from typing import Dict

from inference_tools.type import ParameterType
from kgforge.core import KnowledgeGraphForge

from inference_tools.datatypes.query import ForgeQuery
from inference_tools.helper_functions import _enforce_list, _follow_path, get_id_attribute
from inference_tools.premise_execution import PremiseExecution
from inference_tools.exceptions import InferenceToolsException, MalformedRuleException
from inference_tools.source.source import Source, DEFAULT_LIMIT


class Forge(Source):

    @staticmethod
    def execute_query(forge: KnowledgeGraphForge, query: ForgeQuery,
                      parameter_values: Dict, config, limit=DEFAULT_LIMIT, debug: bool = False):

        q = json.loads(
            Template(json.dumps(query.body)).substitute(**parameter_values)
        )
        return forge.as_json(forge.search(q, debug=debug, limit=limit))

    @staticmethod
    def check_premise(forge: KnowledgeGraphForge, premise: ForgeQuery,
                      parameter_values: Dict, config, debug: bool = False):

        resources = Forge.execute_query(forge=forge, query=premise,
                                        parameter_values=parameter_values, config=config,
                                        debug=debug, limit=None)

        resources = _enforce_list(resources)

        if premise.targetParameter:
            if premise.targetPath:
                try:
                    matched_values = [
                        _follow_path(r, premise.targetPath)
                        for r in resources
                    ]
                except InferenceToolsException:
                    return PremiseExecution.FAIL
            else:
                matched_values = [get_id_attribute(r) for r in resources]

            if parameter_values[premise.targetParameter] not in matched_values:
                return PremiseExecution.FAIL
        else:
            if len(resources) == 0:
                return PremiseExecution.FAIL

            return PremiseExecution.SUCCESS

        raise MalformedRuleException("Missing target parameter in Forge Search Premise")

    @staticmethod
    def restore_default_views(forge: KnowledgeGraphForge):
        pass
