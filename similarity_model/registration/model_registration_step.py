from typing import Callable, List, Any, Tuple, Optional

from similarity_model.registration.logger import logger
from similarity_model.building.model_description import ModelDescription


class ModelRegistrationStep:
    position: int

    function_call: Callable

    log_message: str

    def __init__(self, function_call: Callable, position: int, log_message: str):
        self.position = position
        self.function_call = function_call
        self.log_message = log_message

    def run(self, model_description: ModelDescription, model_revision: Optional[str], **kwargs) -> \
            Any:
        self.log(model_description)
        return self.function_call(model_description=model_description,
                                  model_revision=model_revision, **kwargs)

    def log(self, model_description):
        letter = chr(ord('@') + self.position)
        logger.info(f"{letter}. {self.log_message} for model {model_description.name}")

    def run_alone(self, included_models: List[Tuple[str, ModelDescription]], **kwargs):
        for model_rev, model_desc in included_models:
            # maybe if self.position > 2, check_forge_model, or extend this class with a
            # reimplementation of this function with this check
            self.run(model_description=model_desc, model_revision=model_rev, **kwargs)