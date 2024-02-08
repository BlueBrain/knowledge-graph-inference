from typing import List

from inference_tools.helper_functions import get_type_attribute, get_id_attribute


def _find_derivation_id(derivation_field: List, type_: str) -> str:
    """

    @param derivation_field: the derivation field of an embedding
    @type derivation_field: List
    @param type_: the type of the resource that is one of the derivations
    @type type_:
    @return: the id of the resource that is a derivation, and that has the specified type
    @rtype:
    """
    el = next(
        (
            e for e in derivation_field if type_ in get_type_attribute(e["entity"])
        ), None
    )
    if el is None:
        raise Exception(f"Couldn't find derivation of type {type_} within an embedding")

    return get_id_attribute(el["entity"])
