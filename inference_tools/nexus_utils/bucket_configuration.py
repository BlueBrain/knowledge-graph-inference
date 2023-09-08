from dataclasses import dataclass
from typing import Dict

from kgforge.core import KnowledgeGraphForge

from yaml import safe_load
from urllib.request import urlopen

from inference_tools.helper_functions import get_path
from inference_tools.nexus_utils.forge_utils import ForgeUtils

@dataclass
class BucketConfiguration:
    endpoint: str
    organisation: str
    project: str


class NexusBucketConfiguration(BucketConfiguration):
    token_prod_path = get_path("../token/token_prod.txt")
    token_staging_path = get_path("../token/token_staging.txt")

    config_prod_path = \
       "https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/notebooks/use-cases/prod-forge-nexus.yml"

    endpoint_prod = "https://bbp.epfl.ch/nexus/v1"
    endpoint_staging = "https://staging.nise.bbp.epfl.ch/nexus/v1"

    def __init__(self, organisation: str, project: str, is_prod: bool,
                 elastic_search_view: str = None, sparql_view: str = None,
                 config_file_path: str = None, token_file_path: str = None):

        self.is_prod = is_prod

        endpoint = NexusBucketConfiguration.endpoint_prod if self.is_prod \
            else NexusBucketConfiguration.endpoint_staging

        self.config_file_path = config_file_path
        self.token_file_path = token_file_path

        self.token = None

        self.elastic_search_view = elastic_search_view
        self.sparql_view = sparql_view

        super().__init__(endpoint=endpoint, organisation=organisation, project=project)

    @staticmethod
    def set_token_path_staging(token_path: str):
        NexusBucketConfiguration.token_staging_path = token_path

    def set_token(self, token):
        self.token = token

    @staticmethod
    def set_token_path_prod(token_path: str):
        NexusBucketConfiguration.token_prod_path = token_path

    def get_config(self, bucket) -> Dict:

        with urlopen(self.config_prod_path) as url:
            conf = safe_load(url.read())

        if bucket == "neurosciencegraph/datamodels":
            conf["Model"]["context"]["iri"] = 'https://neuroshapes.org'

        if not self.is_prod:
            conf["Store"]["endpoint"] = NexusBucketConfiguration.endpoint_staging

        return conf

    def get_token_path(self) -> str:
        if self.token_file_path:
            return self.token_file_path
        return NexusBucketConfiguration.token_prod_path if self.is_prod else \
            NexusBucketConfiguration.token_staging_path

    @staticmethod
    def create_from_forge(forge: KnowledgeGraphForge):
        endpoint, org, project = ForgeUtils.get_endpoint_org_project(forge)
        return BucketConfiguration(endpoint=endpoint, organisation=org, project=project)

    @staticmethod
    def load_token(token_file_path: str):
        with open(token_file_path, encoding="utf-8") as f:
            return f.read()

    def allocate_forge_session(self):

        bucket = f"{self.organisation}/{self.project}"

        token = self.token if self.token is not None else \
            NexusBucketConfiguration.load_token(self.get_token_path())

        args = dict(
            configuration=self.get_config(bucket),
            endpoint=self.endpoint,
            token=token,
            bucket=bucket,
            debug=False
        )

        search_endpoints = {}

        if self.elastic_search_view is not None:
            search_endpoints["elastic"] = {"endpoint": self.elastic_search_view}

        if self.sparql_view is not None:
            search_endpoints["sparql"] = {"endpoint": self.sparql_view}

        if len(search_endpoints) > 0:
            args["searchendpoints"] = search_endpoints

        return KnowledgeGraphForge(**args)

