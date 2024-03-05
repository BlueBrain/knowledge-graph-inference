from inference_tools.execution import apply_rule


def test_infer(query_conf, forge_factory):
    q = {
        "@type": "SparqlQuery",
        "hasBody": {"query_string": ""},
        "hasParameter": [],
        "queryConfiguration": query_conf,
        "resultParameterMapping": []
    }

    rule_dict = {
        "@id": "test",
        "@type": "DataGeneralizationRule",
        "description": "Test Rule description",
        "name": "Test rule",
        "searchQuery": q,
        "targetResourceType": "Entity"
    }

    test = apply_rule(forge_factory=forge_factory, parameter_values={}, rule=rule_dict)
