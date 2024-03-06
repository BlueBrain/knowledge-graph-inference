from inference_tools.execution import apply_rule

q2 = {
  "query": {
    "bool": {
        "must_not": {"term": {"hasTarget.hasSource.@id": "$NeuronMorphologyId"}},
        "filter": [
            {"term": {"_deprecated": False}},
            {"term": {"compartment": "$compartment"}}
        ],
        "must": [
          {
            "nested": {
                "path": "hasBody",
                "query": {
                    "bool": {
                        "filter": {"term": {"hasBody.isMeasurementOf.label": "$isMeasurementOf"}},
                        "must":
                            [
                                {
                                    "nested": {
                                        "path": "hasBody.value.series",
                                        "query": {
                                            "script_score": {
                                                "query": {
                                                    "bool": {
                                                        "filter":
                                                            [
                                                                {"term": {"hasBody.value.series.statistic": "$statistic"}}
                                                            ]
                                                    }
                                                },
                                                 "script": {
                                                    "source": """
                                                        1 - (Math.abs(doc['hasBody.value.series.value'].value - params.ref)/ doc['hasBody.value.series.value'].value)
                                                     """,
                                                    "params": {
                                                        "ref": "$value"
                                                    }
                                                }
                                            }

                                        }
                                    }
                                }
                            ]
                    }
                }
           }
         }
        ]
    }
  },
 "track_scores": True,
 "size": "$LimitQueryParameter",
  "_source": ["hasTarget.hasSource.@id"]
}


q1 = """
SELECT ?v WHERE {
    ?ann nsg:hasTarget/nsg:hasSource <$NeuronMorphologyId> .
    ?ann hasBody ?hb .
    ?ann compartment $compartment .
    ?hb isMeasurementOf/label $isMeasurementOf .
    ?hb value/series ?series_element .
    ?series_element statistic $statistic ;
                    value ?v
}
"""

rule = {
    "@context": "https://bbp.neuroshapes.org",
    "@id": "...",
    "@type": "DataGeneralizationRule",
    "description":
        "Given a reference neuron morphology and one of its morphology features "
        "(identifiable by the compartment, the measurement label, and the statistic),"
        "returns neuron morphologies that have similar values for this feature",
    "name": "Neuron Morphology inference based of morphology feature proximity",
    "searchQuery": {
        "@type": "QueryPipe",
        "head": {
            "@type": "SparqlQuery",
            "hasBody": {"query_string": q1},
            "description":
                "Gets the value of the provided neuron morphology's feature, "
                "using the compartment, statistic and measurement label",
            "hasParameter": [
                {
                    "@type": "uri",
                    "description": "The reference neuron morphology",
                    "name": "NeuronMorphologyId",
                },
                {
                    "@type": "str",
                    "description":
                        "In which compartment of the neuron morphology the feature is located",
                    "name": "compartment"
                },
                {
                    "@type": "str",
                    "description": "The statistic of the neuron morphology feature",
                    "name": "statistic"
                },
                {
                    "@type": "str",
                    "description": "The measurement label",
                    "name": "isMeasurementOf"
                },
            ],
            "resultParameterMapping": {
                "parameterName": "value",
                "path": "v"
            },
            "queryConfiguration": {
                "org": "bbp-external",
                "project": "seu",
                "sparqlView": {
                    "@id": "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-sp/dataset"
                }
            }
        },
        "rest": {
            "@type": "ElasticSearchQuery",
            "hasBody": q2,
            "description":
                "Given a value of a feature coming from the neuron morphology of reference, "
                "gets all neuron morphologies whose corresponding feature have the closest value",
            "hasParameter": [
                {
                    "@type": "str",
                    "description":
                        "In which compartment of the neuron morphology the feature is located",
                    "name": "compartment"
                },
                {
                    "@type": "str",
                    "description": "The statistic of the neuron morphology feature",
                    "name": "statistic"
                },
                {
                    "@type": "str",
                    "description": "What the neuron morphology feature measures",
                    "name": "isMeasurementOf"
                },
                {
                    "@type": "path",
                    "description": "The value of the neuron morphology feature",
                    "name": "value"
                },
                {
                    "@type": "str",
                    "description": "The reference neuron morphology",
                    "name": "NeuronMorphologyId"
                },
            ],
            "queryConfiguration": {
                "org": "bbp-external",
                "project": "seu",
                "elasticSearchView": {
                    "@id": "https://bbp.epfl.ch/views/bbp-external/seu/neuron_morphology_feature_annotations_view"
                }
            },
            "resultParameterMapping": [
                {
                    "parameterName": "id",
                    "path": "_source.hasTarget.hasSource.@id"
                },
                {
                    "parameterName": "score",
                    "path": "_score"
                }
            ]

        }
    },
    "targetResourceType": "NeuronMorphology"
}


def test_morpho_metrics_rule(forge_factory):
    parameters = {
        "NeuronMorphologyId": "https://bbp.epfl.ch/neurosciencegraph/data/neuronmorphologies/26617410-d86c-447b-a7ba-6e605fdaf1c7",
        "isMeasurementOf": "Neurite Max Radial Distance",
        "statistic": "raw",
        "compartment": "BasalDendrite"
    }

    temp = apply_rule(
        forge_factory=forge_factory,
        rule=rule,
        parameter_values=parameters,
        premise_check=False, debug=True
    )

    assert temp
    print(temp)
