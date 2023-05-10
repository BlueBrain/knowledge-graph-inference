from inference_tools.bucket_configuration import NexusBucketConfiguration
from inference_tools.source.elastic_search import ElasticSearch
from inference_tools.source.sparql import Sparql
from inference_tools.utils import _allocate_forge_session
from similarity_model.utils import get_path

token_prod_path = get_path("allocate/token_prod.txt")
token_staging_path = get_path("allocate/token_staging.txt")


def get_token_prod():
    with open(token_prod_path, encoding="utf-8") as f:
        return f.read()


def get_token_staging():
    with open(token_staging_path, encoding="utf-8") as f:
        return f.read()


def allocate_forge_session_env(bucket_configuration: NexusBucketConfiguration):
    cp = "../configs/test-config.yaml" if bucket_configuration.is_prod else \
        "../configs/test-config_staging.yaml"

    token_file_path = token_prod_path if bucket_configuration.is_prod else token_staging_path
    config_path = get_path(cp)

    tmp = _allocate_forge_session(
        org=bucket_configuration.organisation,
        project=bucket_configuration.project,
        config_file_path=config_path,
        endpoint=bucket_configuration.endpoint,
        token_file_path=token_file_path
    )

    if bucket_configuration.elastic_search_view is not None:
        ElasticSearch.set_elastic_view(tmp, bucket_configuration.elastic_search_view)
    if bucket_configuration.sparql_view is not None:
        Sparql.set_sparql_view(tmp, bucket_configuration.sparql_view)

    return tmp
