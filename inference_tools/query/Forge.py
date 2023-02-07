import json
from inference_tools.helper_functions import _enforce_list, _follow_path
from string import Template
from inference_tools.PremiseExecution import PremiseExecution
from inference_tools.exceptions import InferenceToolsException
from inference_tools.query.Source import Source


class Forge(Source):

    @staticmethod
    def execute_query(forge, query, parameters, config, debug=False):
        q = json.loads(
            Template(json.dumps(query["pattern"])).substitute(
                **parameters))
        return forge.search(q, debug=debug)

    @staticmethod
    def check_premise(forge, premise, parameters, config, debug=False):
        target_param = premise.get("targetParameter", None)
        target_path = premise.get("targetPath", None)
        query = json.loads(
            Template(json.dumps(premise["pattern"])).substitute(
                **parameters))
        resources = forge.search(query, debug=debug)

        resources = _enforce_list(resources)

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

    @staticmethod
    def restore_default_views(forge):
        pass
