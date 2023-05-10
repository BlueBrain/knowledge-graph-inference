from typing import Callable, List, Any, Tuple, Optional

from similarity_model.registration.logger import logger
from similarity_model.building.model_description import ModelDescription
from similarity_model.registration.step import Step


class ModelRegistrationStep:
    step: Step

    function_call: Callable

    log_message: str

    def __init__(self, function_call: Callable, step: Step, log_message: str):
        self.step = step
        self.function_call = function_call
        self.log_message = log_message

    def run(self, model_information: Tuple[str, ModelDescription], **kwargs) -> Any:
        model_revision, model_description = model_information
        self.log(model_description)
        return self.function_call(model_description=model_description,
                                  model_revision=model_revision, **kwargs)

    def log(self, model_description):
        letter = chr(ord('@') + self.step.value)
        logger.info(f"{letter}. {self.log_message} for model {model_description.name}")

    def run_many(self, models_information: List[Tuple[str, ModelDescription]], **kwargs):
        for model_information in models_information:
            # maybe if self.position > 2, check_forge_model, or extend this class with a
            # reimplementation of this function with this check
            self.run(model_information=model_information, **kwargs)
