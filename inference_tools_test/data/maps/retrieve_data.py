from kgforge.core.wrappings.dict import DictWrapper

from inference_tools_test.data.maps.id_data import make_model_id, revify

retrieve_map = {
    f"{make_model_id(1)}{revify(17)}": DictWrapper({"similarity": "euclidean"}),
    f"{make_model_id(2)}{revify(17)}": DictWrapper({"similarity": "euclidean"})
}
