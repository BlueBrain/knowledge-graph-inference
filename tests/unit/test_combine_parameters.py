from inference_tools.datatypes.parameter_mapping import ParameterMapping

from inference_tools.execution import combine_parameters


def test_combine_parameters():

    make_parameter_mapping = lambda parameter_name, path: \
        ParameterMapping({"parameterName": parameter_name, "path": path})

    result_parameter_mapping = [
        make_parameter_mapping(parameter_name="CombineParamC", path="path1.path2.path3"),
        make_parameter_mapping(parameter_name="CombineParamD", path="path4")
    ]

    res_len = 10

    result = [
        {"path1": {"path2": {"path3": f"{i}_0"}}, "path4":  f"{i}_1"}
        for i in range(res_len)
    ]

    parameter_values = {
        "TestParamA": "ValueA",
        "TestParamB": "ValueB"
    }

    combination = combine_parameters(
        result_parameter_mapping=result_parameter_mapping,
        parameter_values=parameter_values,
        result=result
    )

    expected_combination = {
        "TestParamA": "ValueA",
        "TestParamB": "ValueB",
        "CombineParamC": [f"{i}_0" for i in range(res_len)],
        "CombineParamD": [f"{i}_1" for i in range(res_len)]
    }

    assert combination == expected_combination
