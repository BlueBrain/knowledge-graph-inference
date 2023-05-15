from typing import List, Union, Optional

from inference_tools.type import ObjectTypeStr

from inference_tools.exceptions.exceptions import IncompleteObjectException

from inference_tools.datatypes.query_pipe import QueryPipe
from inference_tools.helper_functions import get_type_attribute, \
    get_id_attribute, _enforce_list
from inference_tools.datatypes.query import query_factory, premise_factory, Query


class Rule:

    name: str
    description: str
    search_query: Union[Query, QueryPipe]
    premises: Optional[List[Query]]
    id: str
    context: str
    type: str
    target_resource_type: str
    nexus_link: Optional[str]

    def __init__(self, obj):
        self.type = get_type_attribute(obj)
        self.context = obj.get("@context", None)
        self.description = obj.get("description", None)
        self.id = get_id_attribute(obj)
        self.target_resource_type = obj.get("targetResourceType", None)
        self.name = obj.get("name", None)
        self.nexus_link = obj.get("nexus_link", None)

        tmp_premise = obj.get("premise", None)
        self.premises = [premise_factory(obj_i) for obj_i in _enforce_list(tmp_premise)] \
            if tmp_premise is not None else None

        tmp_sq = obj.get("searchQuery", None)
        if tmp_sq is None:
            raise IncompleteObjectException(name=self.name,
                                            attribute="searchQuery",
                                            object_type=ObjectTypeStr.RULE)

        self.search_query = query_factory(tmp_sq) \
            if get_type_attribute(tmp_sq) != "QueryPipe" else \
            QueryPipe(tmp_sq)
