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


def test_missing_query_configuration():
    types = ["SparqlQuery", "ForgeSearchQuery", "SimilarityQuery", "ElasticSearchQuery"]

    query_maker = lambda type_: query_factory({
        "type": type_,
        "hasBody": {"query_string": ""}
    })

    expected_error_msg = "The query  has been created with missing mandatory " \
                         "information: queryConfiguration"

    for type_ in types:
        with pytest.raises(IncompleteObjectException, match=expected_error_msg):
            query_maker(type_)


def test_missing_query_has_body():
    pass
    # TODO current implementation doesn't fail if queries do not have a hasBody.
    #  Similarity, Elastic and Sparql should fail if no hasBody is provided.
    #  Forge shouldn't tho
