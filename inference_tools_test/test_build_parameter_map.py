import pytest

from inference_tools.exceptions.exceptions import IncompleteObjectException, \
    MissingPremiseParameterValue, InferenceToolsException

from inference_tools.datatypes.parameter_specification import ParameterSpecification
from inference_tools.type import QueryType, ParameterType, PremiseType
from inference_tools.utils import _build_parameter_map
from inference_tools_test.data.dataclasses.knowledge_graph_forge_test import KnowledgeGraphForgeTest


def make_spec(name: str, type_: str, optional: bool = False, values=None):
    if values is None:
        values = {}

    return ParameterSpecification(obj={
        "name": name,
        "type": type_,
        "optional": optional,
        "values": values
    })


@pytest.fixture
def forge():
    return KnowledgeGraphForgeTest({"org": "bbp", "project": "atlas"})


def test_build_parameter_map_empty(forge):
    parameter_spec = []
    parameter_values = {}

    parameter_map = _build_parameter_map(
        forge=forge,
        parameter_spec=parameter_spec,
        parameter_values=parameter_values,
        query_type=QueryType.SPARQL_QUERY
    )

    assert parameter_map == {}


def test_build_parameter_map_missing_values(forge):
    param_name = "param1"

    parameter_spec = [
        make_spec(
            name=param_name, type_=ParameterType.PATH.value
        )
    ]

    parameter_values = {}

    expected_msg = \
        f"The parameter {param_name} has been created with missing mandatory information: value"

    with pytest.raises(IncompleteObjectException, match=expected_msg):
        _build_parameter_map(
            forge=forge,
            parameter_spec=parameter_spec,
            parameter_values=parameter_values,
            query_type=QueryType.SPARQL_QUERY
        )

    expected_msg2 = 'Premise cannot be ran because parameter param1 has not been provided'
    # Different error raised if dealing with premises
    with pytest.raises(MissingPremiseParameterValue, match=expected_msg2):
        _build_parameter_map(
            forge=forge,
            parameter_spec=parameter_spec,
            parameter_values=parameter_values,
            query_type=PremiseType.SPARQL_PREMISE
        )


def test_build_parameter_map_optional_values(forge):
    param_name = "param1"
    parameter_spec = [
        make_spec(name=param_name, type_=ParameterType.PATH.value, optional=True)
    ]
    parameter_values = {}

    parameter_map = _build_parameter_map(
        forge=forge,
        parameter_spec=parameter_spec,
        parameter_values=parameter_values,
        query_type=QueryType.SPARQL_QUERY
    )
    assert parameter_map == {}


def test_build_parameter_map_restricted_values(forge):
    param_name = "param1"
    valid_values = {
        "a": "aaa",
        "b": "bbb",
        "d": "ddd",
        "e": "eee"
    }

    parameter_spec = [
        make_spec(name=param_name, type_=ParameterType.PATH.value, values=valid_values)
    ]

    parameter_spec2 = [
        make_spec(name=param_name, type_=ParameterType.SPARQL_VALUE_LIST.value, values=valid_values)
    ]

    # Selecting one existing value for a non-list type
    parameter_map1 = _build_parameter_map(
        forge=forge,
        parameter_spec=parameter_spec,
        parameter_values={param_name: "a"},
        query_type=QueryType.SPARQL_QUERY
    )

    assert parameter_map1 == {param_name: "aaa"}

    # Selecting two existing values for a list-type
    parameter_map2 = _build_parameter_map(
        forge=forge,
        parameter_spec=parameter_spec2,
        parameter_values={param_name: ["a", "e"]},
        query_type=QueryType.SPARQL_QUERY
    )
    assert parameter_map2 == {param_name: '("aaa")\n("eee")'}

    # Selecting two existing values for a non-list-type
    parameter_map3 = _build_parameter_map(
        forge=forge,
        parameter_spec=parameter_spec,
        parameter_values={param_name: ["a", "e"]},
        query_type=QueryType.SPARQL_QUERY
    )

    assert parameter_map3 == {param_name: 'aaa'}

    # Selecting a non-existing value
    with pytest.raises(
            InferenceToolsException,
            match=f"Invalid value for parameter {param_name}"
    ):
        _build_parameter_map(
            forge=forge,
            parameter_spec=parameter_spec,
            parameter_values={param_name: "c"},
            query_type=QueryType.SPARQL_QUERY
        )
