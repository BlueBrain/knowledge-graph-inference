import pytest

from inference_tools.execution import apply_rule
from inference_tools_test.data.dataclasses.knowledge_graph_forge_test import KnowledgeGraphForgeTest


@pytest.fixture
def query_conf():
    return {
        "org": "bbp",
        "project": "atlas",
    }


@pytest.fixture
def forge_factory(query_conf):
    return lambda a, b: KnowledgeGraphForgeTest(query_conf)


def test_infer(query_conf, forge_factory):
    q = {
        "@type": "SparqlQuery",
        "hasBody": "",
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
