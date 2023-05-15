from dataclasses import dataclass


@dataclass
class BucketConfiguration:
    endpoint: str
    organisation: str
    project: str


class NexusBucketConfiguration(BucketConfiguration):

    def __init__(self, organisation: str, project: str, is_prod: bool, elastic_search_view=None,
                 sparql_view=None):

        endpoint = "https://bbp.epfl.ch/nexus/v1" \
            if is_prod else "https://staging.nise.bbp.epfl.ch/nexus/v1"

        self.is_prod = is_prod

        self.elastic_search_view = elastic_search_view
        self.sparql_view = sparql_view

        super().__init__(endpoint=endpoint, organisation=organisation, project=project)
