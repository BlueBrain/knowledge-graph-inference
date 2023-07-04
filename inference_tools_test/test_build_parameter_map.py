import pytest

from inference_tools.exceptions.exceptions import IncompleteObjectException, \
    MissingPremiseParameterValue, InferenceToolsException

from inference_tools.datatypes.parameter_specification import ParameterSpecification
from inference_tools.type import QueryType, ParameterType, PremiseType
from inference_tools.utils import _build_parameter_map


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
def param_name():
    return "param1"


@pytest.fixture
def parameter_spec1(param_name):
    return [make_spec(name=param_name, type_=ParameterType.PATH.value, values={
        "a": "aaa",
        "b": "bbb",
        "d": "ddd",
        "e": "eee"
    })]


@pytest.fixture
def parameter_spec2(param_name, parameter_spec1):
    parameter_spec2 = parameter_spec1.copy()
    parameter_spec2[0].type = ParameterType.SPARQL_VALUE_LIST.value
    return parameter_spec2


@pytest.mark.parametrize("parameter_values, parameter_spec, expected_parameter_map", [
    pytest.param(
        {},
        [],
        {},
        id="nothing_no_spec"
    ),
    pytest.param(
        {},
        [make_spec(name=param_name(), type_=ParameterType.PATH.value, optional=True)],
        {},
        id="nothing_optional_true"
    ),
    pytest.param(
        {param_name: "a"},
        parameter_spec1,
        {param_name: "aaa"},
        id="one_value_no_list"
    ),
    pytest.param(
        {param_name: ["a", "e"]},
        parameter_spec1,
        {param_name: 'aaa'},
        id="two_values_no_list"
    ),
    pytest.param(
        {param_name: ["a", "e"]},
        parameter_spec2,
        {param_name: '("aaa")\n("eee")'},
        id="two_values_list"
    )
])
def test_build_parameter_map(
        forge, parameter_spec, parameter_values, expected_parameter_map, param_name
):
    assert _build_parameter_map(
        forge=forge,
        parameter_spec=parameter_spec,
        parameter_values=parameter_values,
        query_type=QueryType.SPARQL_QUERY
    ) == expected_parameter_map


def test_build_parameter_map_select_non_existing_value(forge, parameter_spec1):
    with pytest.raises(
            InferenceToolsException,
            match=f"Invalid value for parameter {param_name}"
    ):
        _build_parameter_map(
            forge=forge,
            parameter_spec=parameter_spec1,
            parameter_values={param_name: "c"},
            query_type=QueryType.SPARQL_QUERY
        )


def test_build_parameter_map_missing_values(forge, param_name):
    parameter_spec = [make_spec(name=param_name, type_=ParameterType.PATH.value)]
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
