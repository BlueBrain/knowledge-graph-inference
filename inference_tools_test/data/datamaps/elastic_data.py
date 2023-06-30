from kgforge.core.wrappings.dict import DictWrapper

test_embedding = {
  "@context": "https://bbp.neuroshapes.org",
  "@id": "https://bbp.epfl.ch/neurosciencegraph/data/embeddings/8de36e4f-150c-49a9-95b5-1658495c268b",
  "@type": [
    "Entity",
    "Embedding"
  ],
  "derivation": {
    "@type": "Derivation",
    "entity": {
      "@id": "https://bbp.epfl.ch/neurosciencegraph/data/neuronmorphologies/7bedc705-acc1-4307-b449-5b6e7cc8e56f",
      "@type": "Entity",
      "hasSelector": {
        "@type": "FragmentSelector",
        "conformsTo": "https://bluebrainnexus.io/docs/delta/api/resources-api.html#fetch",
        "value": "?rev=40"
      }
    }
  },
  "embedding": [
    -0.04658980295062065,
    0.21310146152973175,
    -0.13666123151779175,
    0.04186774790287018,
    -0.00919981487095356,
    -0.1786884218454361,
    0.21865370869636536,
    -0.08203724026679993,
    -0.11935406923294067,
    -0.11642143875360489,
    0.12686726450920105,
    -0.0942830964922905,
    -0.02757236734032631,
    -0.06815221160650253,
    -0.04024111479520798,
    0.17024075984954834,
    0.007321629673242569,
    0.06507324427366257,
    0.09783612936735153,
    0.059310708194971085,
    0.035706982016563416,
    -0.002081784652546048,
    0.005194620694965124,
    -0.14414013922214508,
    0.09318134188652039,
    -0.04492048919200897,
    -0.03190840408205986,
    0.10526187717914581,
    -0.07651037722826004,
    -0.041422050446271896,
    0.03537415340542793,
    -0.032750386744737625,
    -0.12728184461593628,
    0.04679989442229271,
    -0.1211204081773758,
    -0.028140263631939888,
    -0.01491913478821516,
    0.1790141612291336,
    0.11180327832698822,
    -0.0636858120560646,
    -0.07622691243886948,
    0.09263775497674942,
    -0.06749459356069565,
    0.08421863615512848,
    0.020833080634474754,
    0.05929693207144737,
    -0.1181686595082283,
    0.04542633146047592,
    -0.003212584648281336,
    -0.03065897524356842,
    0.08745244145393372,
    -0.009033390320837498,
    0.1786278784275055,
    0.04977160319685936,
    -0.04467008635401726,
    -0.061296455562114716,
    0.01787506602704525,
    0.07793506234884262,
    -0.08705440163612366,
    -0.07889048010110855,
    -0.1186823844909668,
    -0.2622876465320587,
    -0.034074489027261734,
    -0.04991557076573372,
    0.19211351871490479,
    0.07943695783615112,
    -0.10223239660263062,
    -0.014825216494500637,
    0.027822470292448997,
    0.13550807535648346,
    -0.00042610440868884325,
    -0.054408274590969086,
    -0.2276938408613205,
    -0.03245637193322182,
    -0.1753990352153778,
    -0.15571534633636475,
    0.09170682728290558,
    0.009672611020505428,
    -0.06179286912083626,
    -0.04346013441681862,
    -0.01609363965690136,
    0.02306145615875721,
    0.18328259885311127,
    0.13332943618297577,
    -0.13496771454811096,
    0.012931646779179573,
    0.10672350972890854,
    0.0875544548034668,
    -0.009539850987493992,
    0.12916746735572815,
    0.08783824741840363,
    0.06202339380979538,
    -0.06085563078522682,
    -0.07028167694807053,
    -0.0019328329944983125,
    0.23572541773319244,
    0.0728016272187233,
    0.013034284114837646,
    -0.07241810113191605,
    0.09632326662540436
  ],
  "generation": {
    "@type": "Generation",
    "activity": {
      "@type": [
        "Activity",
        "EmbeddingActivity"
      ],
      "used": {
        "@id": "https://bbp.epfl.ch/nexus/v1/resources/dke/embedding-pipelines/_/9fe6873b-ef6a-41b5-854a-382bc1be9fff",
        "@type": "EmbeddingModel",
        "hasSelector": {
          "@type": "FragmentSelector",
          "conformsTo": "https://bluebrainnexus.io/docs/delta/api/resources-api.html#fetch",
          "value": "?rev=13"
        }
      },
      "wasAssociatedWith": {
        "@type": "SoftwareAgent",
        "description": "Unifying Python framework for graph analytics and co-occurrence analysis.",
        "name": "BlueGraph",
        "softwareSourceCode": {
          "@type": "SoftwareSourceCode",
          "codeRepository": "https://github.com/BlueBrain/BlueGraph",
          "programmingLanguage": "Python",
          "runtimePlatform": 3.7,
          "version": "v0.1.2"
        }
      }
    }
  },
  "name": [
    "Embedding of 7bedc705-acc1-4307-b449-5b6e7cc8e56f at revision 40"
  ]
}

test_embedding_res = DictWrapper(test_embedding)

test_embedding2 = test_embedding.copy()
test_embedding2_res = DictWrapper(test_embedding2)
test_embedding2_res.__dict__["_store_metadata"] = DictWrapper({"_score": 0.5})

get_embedding_vector_query = """{"from": 0, "size": 1, "query": {"bool": {"must": [{"nested": {"path": "derivation.entity", "query": {"terms": {"derivation.entity.@id": ["any"]}}}}]}}}"""
get_neighbors_query = """{"from": 0, "size": 50, "query": {"script_score": {"query": {"bool": {"must_not": {"term": {"@id": "https://bbp.epfl.ch/neurosciencegraph/data/embeddings/8de36e4f-150c-49a9-95b5-1658495c268b"}}, "must": {"exists": {"field": "embedding"}}}}, "script": {"source": "if (doc['embedding'].size() == 0) { return 0; } double d = l2norm(params.query_vector, 'embedding'); return (1 / (1 + d))", "params": {"query_vector": [-0.04658980295062065, 0.21310146152973175, -0.13666123151779175, 0.04186774790287018, -0.00919981487095356, -0.1786884218454361, 0.21865370869636536, -0.08203724026679993, -0.11935406923294067, -0.11642143875360489, 0.12686726450920105, -0.0942830964922905, -0.02757236734032631, -0.06815221160650253, -0.04024111479520798, 0.17024075984954834, 0.007321629673242569, 0.06507324427366257, 0.09783612936735153, 0.059310708194971085, 0.035706982016563416, -0.002081784652546048, 0.005194620694965124, -0.14414013922214508, 0.09318134188652039, -0.04492048919200897, -0.03190840408205986, 0.10526187717914581, -0.07651037722826004, -0.041422050446271896, 0.03537415340542793, -0.032750386744737625, -0.12728184461593628, 0.04679989442229271, -0.1211204081773758, -0.028140263631939888, -0.01491913478821516, 0.1790141612291336, 0.11180327832698822, -0.0636858120560646, -0.07622691243886948, 0.09263775497674942, -0.06749459356069565, 0.08421863615512848, 0.020833080634474754, 0.05929693207144737, -0.1181686595082283, 0.04542633146047592, -0.003212584648281336, -0.03065897524356842, 0.08745244145393372, -0.009033390320837498, 0.1786278784275055, 0.04977160319685936, -0.04467008635401726, -0.061296455562114716, 0.01787506602704525, 0.07793506234884262, -0.08705440163612366, -0.07889048010110855, -0.1186823844909668, -0.2622876465320587, -0.034074489027261734, -0.04991557076573372, 0.19211351871490479, 0.07943695783615112, -0.10223239660263062, -0.014825216494500637, 0.027822470292448997, 0.13550807535648346, -0.00042610440868884325, -0.054408274590969086, -0.2276938408613205, -0.03245637193322182, -0.1753990352153778, -0.15571534633636475, 0.09170682728290558, 0.009672611020505428, -0.06179286912083626, -0.04346013441681862, -0.01609363965690136, 0.02306145615875721, 0.18328259885311127, 0.13332943618297577, -0.13496771454811096, 0.012931646779179573, 0.10672350972890854, 0.0875544548034668, -0.009539850987493992, 0.12916746735572815, 0.08783824741840363, 0.06202339380979538, -0.06085563078522682, -0.07028167694807053, -0.0019328329944983125, 0.23572541773319244, 0.0728016272187233, 0.013034284114837646, -0.07241810113191605, 0.09632326662540436]}}}}}"""

elastic_patterns = [
    (lambda q: q == get_embedding_vector_query, [test_embedding_res]),
    (lambda q: q == get_neighbors_query, [test_embedding2_res])
]
