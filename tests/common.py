from enum import Enum
import base64
import requests

from kgforge.core import KnowledgeGraphForge


class Deployment(Enum):
    STAGING = "https://staging.nise.bbp.epfl.ch/nexus/v1"
    PRODUCTION = "https://bbp.epfl.ch/nexus/v1"
    AWS = "https://sbo-nexus-delta.shapes-registry.org/v1"


def auth(username, password, realm, server_url):

    def basic_auth():
        token = base64.b64encode(f"{username}:{password}".encode('utf-8')).decode("ascii")
        return f'Basic {token}'

    url = f"{server_url}/realms/{realm}/protocol/openid-connect/token"

    resp = requests.post(
        url=url,
        headers={
            'Content-Type': "application/x-www-form-urlencoded",
            'Authorization': basic_auth()
        },
        data={
            'grant_type': "client_credentials",
            'scope': "openid"
        }
    )

    response_json = resp.json()

    return response_json['access_token']


def init_forge(
        token, org, project, es_view=None, sparql_view=None, deployment=Deployment.PRODUCTION
):

    bucket = f"{org}/{project}"
    config = "https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/notebooks/use-cases/prod-forge-nexus.yml"

    args = dict(
        configuration=config,
        endpoint=deployment.value,
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
