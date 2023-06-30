from kgforge.core.wrappings.dict import DictWrapper

from inference_tools_test.data.dataclasses.knowledge_graph_forge_test import ResourceTest

retrieve_map = {
    "embedding_model_id?rev=17": ResourceTest(DictWrapper({"similarity": "euclidean"}))
}