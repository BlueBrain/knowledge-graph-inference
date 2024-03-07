import pytest

from tests.common import auth, init_forge, Deployment

RULE_ORG, RULE_PROJ = "bbp", "inference-rules"
RULE_ES_VIEW = "https://bbp.epfl.ch/bbp/inference-rules/views/aggreg-es/rule_view_no_tag"
RULE_SP_VIEW = "https://bbp.epfl.ch/bbp/inference-rules/views/aggreg-sp/rule_view_no_tag"


def pytest_addoption(parser):
    parser.addoption("--username_aws", action="store")
    parser.addoption("--password_aws", action="store")
    parser.addoption("--token_aws", action="store", default=None)


@pytest.fixture(scope="session")
def token(pytestconfig):
    provided_token = pytestconfig.getoption("token_aws")

    if provided_token is not None and len(provided_token) > 0:
        return provided_token

    username = pytestconfig.getoption("username_aws")
    password = pytestconfig.getoption("password_aws")

    if not username or not password:
        raise Exception("Missing command line parameters username_aws and password_aws")

    return auth(username, password, realm="SBO", server_url="https://sboauth.epfl.ch/auth")


@pytest.fixture
def rule_forge(token):
    return init_forge(
        token, RULE_ORG, RULE_PROJ, RULE_ES_VIEW, RULE_SP_VIEW, deployment=Deployment.AWS
    )


@pytest.fixture
def forge_factory(token):
    return lambda org, proj, es=None, sp=None: init_forge(
        token, org, proj, es, sp, deployment=Deployment.AWS
    )
