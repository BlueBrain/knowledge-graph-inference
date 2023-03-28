from typing import Union

from inference_tools.helper_functions import get_type_attribute
from inference_tools.datatypes.query import query_factory, Query


class QueryPipe:
    head: Query
    rest: Union[Query, 'QueryPipe']

    def __init__(self, obj):
        self.head = query_factory(obj["head"])
        tmp = obj["rest"]
        self.rest = query_factory(tmp) if get_type_attribute(tmp) != "QueryPipe" else \
            QueryPipe(tmp)
