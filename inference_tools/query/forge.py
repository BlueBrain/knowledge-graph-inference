import json
from string import Template
from inference_tools.helper_functions import _enforce_list, _follow_path
from inference_tools.premise_execution import PremiseExecution
from inference_tools.exceptions import InferenceToolsException
from inference_tools.query.source import Source


class Forge(Source):

    @staticmethod
    def execute_query(forge, query, parameters, config=None, debug=False):

        q = json.loads(
            Template(json.dumps(query["pattern"])).substitute(**parameters)
        )
        return forge.search(q, debug=debug)

    @staticmethod
    def check_premise(forge, premise, parameters, config, debug=False):

        resources = Forge.execute_query(forge, premise, parameters, config, debug)

        resources = _enforce_list(resources)

        target_param = premise.get("targetParameter", None)
        target_path = premise.get("targetPath", None)

        if target_param:
            if target_path:
                try:
                    matched_values = [
                        _follow_path(forge.as_json(r), target_path)
                        for r in resources
                    ]
                except InferenceToolsException:
                    return PremiseExecution.FAIL
            else:
                matched_values = [r.id for r in resources]

            if parameters[target_param] not in matched_values:
                return PremiseExecution.FAIL
        else:
            if len(resources) == 0:
                return PremiseExecution.FAIL

            return PremiseExecution.SUCCESS

        raise InferenceToolsException("Missing target parameter")

    @staticmethod
    def restore_default_views(forge):
        pass
