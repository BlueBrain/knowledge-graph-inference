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
from contextlib import nullcontext as does_not_raise

from inference_tools.exceptions.exceptions import SimilaritySearchException

from inference_tools.datatypes.query import query_factory
from inference_tools.execution import execute_query_object
from inference_tools.similarity.queries.get_embedding_vector import _err_message

from tests.data.maps.id_data import (
    make_model_id,
    make_entity_id,
    make_org,
    make_project
)


@pytest.fixture
def similarity_search_query_single():
    return {
        "@type": "SimilarityQuery",
        "hasParameter": [
            {
                "@type": "uri",
                "description": "test param",
                "name": "TargetResourceParameter"
            }
        ],
        "k": 50,
        "queryConfiguration": [
            {
                "boosted": True,
                "boostingView": {
                    "@id": "boosting_view_id",
                    "@type": "ElasticSearchView"
                },
                "description": "Model description",
                "embeddingModelDataCatalog": {
                    "@id": make_model_id(1),
                    "@type": "EmbeddingModelDataCatalog",
                    "_rev": 1,
                    "org": make_org(1),
                    "project": make_project(1),
                    "distance": "euclidean",
                    "about": "Entity",
                    "name": "Model name",
                    "description": "Model description"
                },
                "org": make_org(1),
                "project": make_project(1),
                "similarityView": {
                    "@id": "similarity_view_id",
                    "@type": "ElasticSearchView"
                },
                "statisticsView": {
                    "@id": "stat_view_id",
                    "@type": "ElasticSearchView"
                }
            }
        ],
        "searchTargetParameter": "TargetResourceParameter"
    }


@pytest.mark.parametrize(
    "entity_uuid, expectation",
    [
        pytest.param(1, does_not_raise(), id=str(1)),
        pytest.param(2, does_not_raise(), id=str(2)),
        pytest.param(3,  does_not_raise(), id=str(3)),
        pytest.param(11,  pytest.raises(
            SimilaritySearchException,
            match=_err_message(make_entity_id(11), model_name="Model name")
        ), id=str(11)),
    ]
)
def test_execute_single(forge_factory, similarity_search_query_single, entity_uuid, expectation):

    with expectation:
        e = execute_query_object(
            query=query_factory(similarity_search_query_single),
            forge_factory=forge_factory,
            parameter_values={"TargetResourceParameter": make_entity_id(entity_uuid)},
            use_resources=True
        )


@pytest.fixture()
def similarity_search_query_combine(similarity_search_query_single):
    new_query = similarity_search_query_single.copy()
    q2 = new_query["queryConfiguration"][0].copy()
    # q2["org"] = make_org(1)
    # q2["project"] = make_project(1)
    # q2["embeddingModel"]["@id"] = make_model_id(1)
    new_query["queryConfiguration"].append(q2)
    return new_query


# TODO
# def test_execute_combine(forge_factory, similarity_search_query_combine):
#     e = execute_query_object(
#         query=query_factory(similarity_search_query_combine),
#         forge_factory=forge_factory,
#         parameter_values={"TargetResourceParameter": make_entity_id(1)},
#         use_resources=True
#     )


