from inference_tools.execution import apply_rule
from inference_tools.source.elastic_search import ElasticSearch


def test_neuron_morphology_rule(forge_factory, rule_forge):

    parameters = {
        'TargetResourceParameter': "https://bbp.epfl.ch/neurosciencegraph/data/1555e794-8c8a-4d1d-a0fd-bfad84e21e73",
        'SelectModelsParameter': [
            "Unscaled_Topology_Morphology_Descriptor-based_similarity",
        ],
        'LimitQueryParameter': 100
    }

    rule_id = "https://bbp.epfl.ch/neurosciencegraph/data/abb1949e-dc16-4719-b43b-ff88dabc4cb8"
    rule = rule_forge.as_json(ElasticSearch.get_by_id(ids=rule_id, forge=rule_forge))

    res = apply_rule(
        forge_factory=forge_factory,
        rule=rule,
        parameter_values=dict(parameters),
        premise_check=False, debug=False
    )

    assert len(res) > 0
