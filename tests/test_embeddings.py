"""Collection of data resources for testing embedding models."""


BRAIN_REGION_MODEL_ID = "https://bbp.epfl.ch/neurosciencegraph/data/BrainRegionEmbeddingModel"
BRAIN_REGION_MODEL = {
    "@id": BRAIN_REGION_MODEL_ID,
    "@type": "EmbeddingModel",
    "name": "Brain Region Hierarchical Embedding Model",
    "prefLabel": "Brain region model",
    "similarity": "euclidean",
    "vectorDimension": 2
}


BRAIN_REGION_EMBEDDING = {
    "https://neuroshapes.org/BrainRegion": [2, 1],
    "http://api.brain-map.org/api/v2/data/Structure/315": [3, 2],
    "http://api.brain-map.org/api/v2/data/Structure/44": [3.2, 2.8],
    "http://api.brain-map.org/api/v2/data/Structure/22": [3.8, 1.8],
    "http://api.brain-map.org/api/v2/data/Structure/31": [2.8, 3.2],
    "http://api.brain-map.org/api/v2/data/Structure/48": [2.6, 3],
    "http://api.brain-map.org/api/v2/data/Structure/549": [1, 2]
}


MTYPE_MODEL1_ID = "https://bbp.epfl.ch/neurosciencegraph/data/MTypeEmbeddingModel1"
MTYPE_MODEL1 = {
    "@id": MTYPE_MODEL1_ID,
    "@type": "EmbeddingModel",
    "name": "MType Embedding Model",
    "prefLabel": "MType model",
    "similarity": "euclidean",
    "vectorDimension": 2
}

MTYPE_EMBEDDING1 = {
    "https://neuroshapes.org/MType": [0, 0],
    "https://neuroshapes.org/PyramidalNeuron": [0.5, 2.5],
    "https://neuroshapes.org/TufterdPyramidalNeuron": [1, 4],
    "https://neuroshapes.org/UntufterdPyramidalNeuron": [0.5, 3.8],
    "http://uri.interlex.org/base/ilx_0381371": [0.4, 4.3],
    "http://uri.interlex.org/base/ilx_0381377": [0.6, 4.5],
    "https://neuroshapes.org/MartinottiCell": [1, 1.5],
    "http://uri.interlex.org/base/ilx_0738235": [2, 0]
}


MTYPE_MODEL2_ID = "https://bbp.epfl.ch/neurosciencegraph/data/MTypeEmbeddingModel2"
MTYPE_MODEL2 = {
    "@id": MTYPE_MODEL2_ID,
    "@type": "EmbeddingModel",
    "name": "MType Embedding Model 2",
    "prefLabel": "MType model 2",
    "similarity": "euclidean",
    "vectorDimension": 2
}

MTYPE_EMBEDDING2 = {
    "https://neuroshapes.org/MType": [1.5, 1],
    "https://neuroshapes.org/PyramidalNeuron": [1.5, 2],
    "https://neuroshapes.org/TufterdPyramidalNeuron": [2.5, 2],
    "https://neuroshapes.org/UntufterdPyramidalNeuron": [2, 2.5],
    "http://uri.interlex.org/base/ilx_0381371": [1, 4],
    "http://uri.interlex.org/base/ilx_0381377": [4, 3.5],
    "https://neuroshapes.org/MartinottiCell": [3, 1],
    "http://uri.interlex.org/base/ilx_0738235": [3.5, 0.5]
}


TRACE_MODEL_ID = "https://bbp.epfl.ch/neurosciencegraph/data/TraceEmbeddingModel"
TRACE_MODEL = {
    "@id": TRACE_MODEL_ID,
    "@type": "EmbeddingModel",
    "name": "Trace Embedding Model",
    "prefLabel": "Trace model",
    "similarity": "euclidean",
    "vectorDimension": 2
}
TRACE_EMBEDDINGS = {
    "https://bbp.epfl.ch/neurosciencegraph/data/Trace1": [1, 1],
    "https://bbp.epfl.ch/neurosciencegraph/data/Trace2": [1, 2],
    "https://bbp.epfl.ch/neurosciencegraph/data/Trace3": [2, 3],
    "https://bbp.epfl.ch/neurosciencegraph/data/Trace4": [6, 1],
    "https://bbp.epfl.ch/neurosciencegraph/data/Trace5": [4, 3]
}