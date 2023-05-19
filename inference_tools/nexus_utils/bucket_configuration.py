from dataclasses import dataclass

from inference_tools.helper_functions import get_path
from inference_tools.nexus_utils.forge_utils import ForgeUtils
from kgforge.core import KnowledgeGraphForge


@dataclass
class BucketConfiguration:
    endpoint: str
    organisation: str
    project: str


class NexusBucketConfiguration(BucketConfiguration):
    token_prod_path = get_path("../token/token_prod.txt")
    token_staging_path = get_path("../token/token_staging.txt")

    config_prod_path = get_path("../configs/test-config.yaml")
    config_staging_path = get_path("../configs/test-config_staging.yaml")

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

        self.elastic_search_view = elastic_search_view
        self.sparql_view = sparql_view

        super().__init__(endpoint=endpoint, organisation=organisation, project=project)

    @staticmethod
    def set_token_path_staging(token_path: str):
        NexusBucketConfiguration.token_staging_path = token_path

    @staticmethod
    def set_token_path_prod(token_path: str):
        NexusBucketConfiguration.token_prod_path = token_path

    def get_config_path(self):
        if self.config_file_path:
            return self.config_file_path
        return NexusBucketConfiguration.config_prod_path if self.is_prod else \
            NexusBucketConfiguration.config_staging_path

    def get_token_path(self):
        if self.token_file_path:
            return self.token_file_path
        return NexusBucketConfiguration.token_prod_path if self.is_prod else \
            NexusBucketConfiguration.token_staging_path

    @staticmethod
    def create_from_forge(forge: KnowledgeGraphForge):
        endpoint, org, project = ForgeUtils.get_org_proj_endpoint(forge)
        return BucketConfiguration(endpoint=endpoint, organisation=org, project=project)

    @staticmethod
    def load_token(token_file_path: str):
        with open(token_file_path, encoding="utf-8") as f:
            return f.read()

    def allocate_forge_session(self):

        tmp = KnowledgeGraphForge(
            configuration=self.get_config_path(),
            endpoint=self.endpoint,
            token=NexusBucketConfiguration.load_token(self.get_token_path()),
            bucket=f"{self.organisation}/{self.project}",
            debug=False
        )

        if self.sparql_view is not None:
            ForgeUtils.set_sparql_view(tmp, self.sparql_view)

        if self.elastic_search_view is not None:
            ForgeUtils.set_elastic_search_view(tmp, self.elastic_search_view)

        return tmp
