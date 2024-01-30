import pytest
import requests
import base64
from kgforge.core import KnowledgeGraphForge


RULE_ORG, RULE_PROJ = "bbp", "inference-rules"
RULE_ES_VIEW = "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-es/rule_view_no_tag"
RULE_SP_VIEW = "https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-sp/rule_view_no_tag"


def pytest_addoption(parser):
    parser.addoption("--username", action="store")
    parser.addoption("--password", action="store")
    parser.addoption("--token", action="store", default=None)


@pytest.fixture(scope="session")
def token(pytestconfig):

    provided_token = pytestconfig.getoption("token")

    if provided_token is not None and len(provided_token) > 0:
        return provided_token

    username = pytestconfig.getoption("username")
    password = pytestconfig.getoption("password")

    def basic_auth(username, password):
        token = base64.b64encode(f"{username}:{password}".encode('utf-8')).decode("ascii")
        return f'Basic {token}'

    url = "https://bbpauth.epfl.ch/auth/realms/BBP/protocol/openid-connect/token"

    resp = requests.post(
        url=url,
        headers={
            'Content-Type': "application/x-www-form-urlencoded",
            'Authorization': basic_auth(username, password)
        },
        data={
            'grant_type': "client_credentials",
            'scope': "openid"
        }
    )

    return resp.json()['access_token']


@pytest.fixture
def rule_forge(token):
    return init_forge(token, RULE_ORG, RULE_PROJ, RULE_ES_VIEW, RULE_SP_VIEW)


@pytest.fixture
def forge_factory(token):
    return lambda org, proj, es=None, sp=None: init_forge(token, org, proj, es, sp)


def init_forge(token, org, project, es_view=None, sparql_view=None):

    bucket = f"{org}/{project}"
    config = "https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/notebooks/use-cases/prod-forge-nexus.yml"

    args = dict(
        configuration=config,
        token=token,
        bucket=bucket,
        debug=False
    )

    search_endpoints = {}

    if es_view is not None:
        search_endpoints["elastic"] = {"endpoint": es_view}

    if sparql_view is not None:
        search_endpoints["sparql"] = {"endpoint": sparql_view}

    if len(search_endpoints) > 0:
        args["searchendpoints"] = search_endpoints

    return KnowledgeGraphForge(**args)
