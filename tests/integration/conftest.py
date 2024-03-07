import pytest

from tests.common import auth, init_forge

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

    if not username or not password:
        raise Exception("Missing command line parameters username and password")

    server_url = "https://bbpauth.epfl.ch/auth"
    realm = "BBP"

    return auth(username=username, password=password, server_url=server_url, realm=realm)


@pytest.fixture
def rule_forge(token):
    return init_forge(token, RULE_ORG, RULE_PROJ, RULE_ES_VIEW, RULE_SP_VIEW)


@pytest.fixture
def forge_factory(token):
    return lambda org, proj, es=None, sp=None: init_forge(token, org, proj, es, sp)
