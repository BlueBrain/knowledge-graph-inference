# This file is part of knowledge-graph-inference.
# Copyright 2024 Blue Brain Project / EPFL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
