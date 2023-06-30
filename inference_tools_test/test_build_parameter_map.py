import unittest

from inference_tools.exceptions.exceptions import IncompleteObjectException, \
    MissingPremiseParameterValue, InferenceToolsException

from inference_tools.datatypes.parameter_specification import ParameterSpecification
from inference_tools.type import QueryType, ParameterType, PremiseType
from inference_tools.utils import _build_parameter_map
from inference_tools_test.data.dataclasses.knowledge_graph_forge_test import KnowledgeGraphForgeTest


class BuildParameterMapTest(unittest.TestCase):

    @staticmethod
    def make_spec(name: str, type_: str, optional: bool = False, values=None):
        if values is None:
            values = {}

        return ParameterSpecification(obj={
            "name": name,
            "type": type_,
            "optional": optional,
            "values": values
        })

    def setUp(self) -> None:
        self.forge = KnowledgeGraphForgeTest()

    def test_build_parameter_map_empty(self):

        forge = KnowledgeGraphForgeTest()

        parameter_spec = []
        parameter_values = {}

        parameter_map = _build_parameter_map(
            forge=forge,
            parameter_spec=parameter_spec,
            parameter_values=parameter_values,
            query_type=QueryType.SPARQL_QUERY
        )

        self.assertDictEqual(parameter_map, {})

    def test_build_parameter_map_missing_values(self):

        param_name = "param1"

        parameter_spec = [
            BuildParameterMapTest.make_spec(
                name=param_name, type_=ParameterType.PATH.value
            )
        ]

        parameter_values = {}

        expected_msg = \
            f"The parameter {param_name} has been created with missing mandatory information: value"

        with self.assertRaises(IncompleteObjectException, msg=expected_msg):
            _build_parameter_map(
                forge=self.forge,
                parameter_spec=parameter_spec,
                parameter_values=parameter_values,
                query_type=QueryType.SPARQL_QUERY
            )

        # Different error raised if dealing with premises
        with self.assertRaises(MissingPremiseParameterValue, msg=expected_msg):
            _build_parameter_map(
                forge=self.forge,
                parameter_spec=parameter_spec,
                parameter_values=parameter_values,
                query_type=PremiseType.SPARQL_PREMISE
            )

    def test_build_parameter_map_optional_values(self):

        param_name = "param1"
        parameter_spec = [
            BuildParameterMapTest.make_spec(
                name=param_name, type_=ParameterType.PATH.value, optional=True
            )
        ]
        parameter_values = {}

        parameter_map = _build_parameter_map(
            forge=self.forge,
            parameter_spec=parameter_spec,
            parameter_values=parameter_values,
            query_type=QueryType.SPARQL_QUERY
        )
        self.assertDictEqual(parameter_map, {})

    def test_build_parameter_map_restricted_values(self):

        param_name = "param1"
        valid_values = {
            "a": "aaa",
            "b": "bbb",
            "d": "ddd",
            "e": "eee"
        }

        parameter_spec = [
            self.make_spec(name=param_name, type_=ParameterType.PATH.value, values=valid_values)
        ]

        parameter_spec2 = [
            self.make_spec(
                name=param_name, type_=ParameterType.SPARQL_VALUE_LIST.value, values=valid_values
            )
        ]

        # Selecting one existing value for a non-list type
        parameter_map1 = _build_parameter_map(
            forge=self.forge,
            parameter_spec=parameter_spec,
            parameter_values={param_name: "a"},
            query_type=QueryType.SPARQL_QUERY
        )

        self.assertDictEqual(parameter_map1, {param_name: "aaa"})

        # Selecting two existing values for a list-type
        parameter_map2 = _build_parameter_map(
            forge=self.forge,
            parameter_spec=parameter_spec2,
            parameter_values={param_name: ["a", "e"]},
            query_type=QueryType.SPARQL_QUERY
        )
        self.assertDictEqual(parameter_map2, {param_name: '("aaa")\n("eee")'})

        # Selecting two existing values for a non-list-type
        parameter_map3 = _build_parameter_map(
            forge=self.forge,
            parameter_spec=parameter_spec,
            parameter_values={param_name: ["a", "e"]},
            query_type=QueryType.SPARQL_QUERY
        )

        self.assertDictEqual(parameter_map3, {param_name: 'aaa'})

        # Selecting a non-existing value
        with self.assertRaises(
                InferenceToolsException,
                msg=f"Invalid value for parameter {param_name}"
        ):

            _build_parameter_map(
                forge=self.forge,
                parameter_spec=parameter_spec,
                parameter_values={param_name: "c"},
                query_type=QueryType.SPARQL_QUERY
            )
