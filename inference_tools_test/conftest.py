import pytest

from inference_tools_test.data.classes.knowledge_graph_forge_test import KnowledgeGraphForgeTest


@pytest.fixture(scope="session")
def query_conf():
    return {
        "org": "bbp",
        "project": "atlas",
    }


@pytest.fixture(scope="session")
def forge_factory():
    return lambda a, b, c, d: KnowledgeGraphForgeTest({"org": a, "project": b})


@pytest.fixture(scope="session")
def forge(query_conf):
    return KnowledgeGraphForgeTest(query_conf)
