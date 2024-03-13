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

import pytest

from tests.data.classes.knowledge_graph_forge_test import KnowledgeGraphForgeTest


@pytest.fixture(scope="session")
def query_conf():
    return {
        "org": "bbp",
        "project": "atlas",
    }


@pytest.fixture(scope="session")
def forge_factory():
    return lambda a, b, c, d: KnowledgeGraphForgeTest({"org": a, "project": b})


@pytest.fixture(scope="session")
def forge(query_conf):
    return KnowledgeGraphForgeTest(query_conf)
