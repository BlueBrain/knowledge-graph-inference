def make_model_id(i: int):
    return _make_id(i=i, type_="model")


def make_embedding_id(i: int):
    return _make_id(i=i, type_="embedding")


def make_entity_id(i: int):
    return _make_id(i=i, type_="entity")


def _make_id(i: int, type_: str):
    return f"https://bbp.epfl.ch/{type_}_{i}"


def revify(i: int):
    return f"?rev={i}"


def make_org(i: int):
    return f"org_{i}"


def make_project(i: int):
    return f"project_{i}"
