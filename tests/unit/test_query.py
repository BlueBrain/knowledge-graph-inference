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

from inference_tools.datatypes.query import query_factory, SparqlQuery, ElasticSearchQuery, \
    SimilaritySearchQuery, ForgeQuery
from inference_tools.exceptions.exceptions import InvalidValueException, IncompleteObjectException
from inference_tools.type import QueryType


def test_query_type(query_conf):
    types = {
        QueryType.SPARQL_QUERY.value: SparqlQuery,
        QueryType.FORGE_SEARCH_QUERY: ForgeQuery,
        QueryType.SIMILARITY_QUERY: SimilaritySearchQuery,
        QueryType.ELASTIC_SEARCH_QUERY: ElasticSearchQuery
    }

    query_maker = lambda type_: query_factory({
        "type": type_,
        "queryConfiguration": query_conf,
        "hasBody": {"query_string": ""}
    })

    for type_, class_ in types.items():
        query = query_maker(type_)
        assert isinstance(query, class_)

    with pytest.raises(InvalidValueException):
        query_maker("InvalidType")


@pytest.mark.parametrize(
    "query_type",
    [
        pytest.param("SparqlQuery", id="SparqlQuery"),
        pytest.param("ForgeSearchQuery", id="ForgeSearchQuery"),
        pytest.param("SimilarityQuery", id="SimilarityQuery"),
        pytest.param("ElasticSearchQuery", id="ElasticSearchQuery")
    ]
)
def test_missing_query_configuration(query_type):

    expectation = pytest.raises(
        IncompleteObjectException,
        match=
        "The query  has been created with missing mandatory information: queryConfiguration"
    )

    with expectation:
        query_factory({
            "type": query_type,
            "hasBody": {"query_string": ""}
        })


def test_missing_query_has_body():
    pass
    # TODO current implementation doesn't fail if queries do not have a hasBody.
    #  Similarity, Elastic and Sparql should fail if no hasBody is provided.
    #  Forge shouldn't tho
