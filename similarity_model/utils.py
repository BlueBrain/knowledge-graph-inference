"""Utils for registering similarity-related resources in Nexus."""
from typing import Tuple, Optional
from urllib import parse
import os
from collections import namedtuple
from kgforge.core import KnowledgeGraphForge


def encode_id_rev(resource_id: str, resource_rev: str):
    return f"{resource_id}?{parse.urlencode({'rev': resource_rev})}"


def parse_id_rev(resource_str: str) -> Tuple[str, Optional[str]]:
    s = resource_str.split('?', 1)
    rev = dict(parse.parse_qsl(s[1]))["rev"] if len(s) > 1 else None
    return s[0], rev


def get_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_model_tag(model_id, model_revision):
    model_uuid = model_id.split('/')[-1]
    return f"{model_uuid}?rev={model_revision}"


def get_stat_view_id(model):
    return get_x_view_id("stat", model)


def get_boosting_view_id(model):
    return get_x_view_id("boosting", model)


def get_similarity_view_id(model):
    return get_x_view_id("similarity", model)


def get_x_view_id(x: str, model):
    name = model.prefLabel.replace("(", "").replace(")", "").replace(" ", "_").lower()
    view_name = f"{name}_{x}_view"
    return f"https://bbp.epfl.ch/neurosciencegraph/data/views/es/{view_name}"


BucketConfiguration = namedtuple('BucketConfiguration', 'endpoint org proj')


def create_forge_session(config_path, bucket_config, token):
    """Create a forge session."""
    endpoint, org, proj = bucket_config
    return KnowledgeGraphForge(
        config_path,
        token=token,
        endpoint=endpoint,
        bucket=f"{org}/{proj}")


def add_views_with_replacement(existing_views, new_views):
    """Add new views with replacement."""
    new_views = {el["_project"]: el for el in new_views}
    existing_views = {el["_project"]: el for el in existing_views}
    existing_views.update(new_views)
    return list(existing_views.values())
