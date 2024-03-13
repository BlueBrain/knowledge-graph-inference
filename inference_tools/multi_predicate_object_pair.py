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

from typing import Dict, List, Optional, Tuple, Any

from inference_tools.exceptions.malformed_rule import InvalidParameterTypeException, \
    MalformedRuleException

from inference_tools.datatypes.parameter_specification import ParameterSpecification
from inference_tools.datatypes.query import SparqlQueryBody, Query, SparqlQuery
from inference_tools.type import ParameterType


def multi_predicate_object_pairs_query_rewriting(
        name: str, nb_multi: int, query_body: SparqlQueryBody
) -> SparqlQueryBody:
    """
    Rewrite the query in order to have the line where the parameter of name "name"
    duplicated for as many predicate-pairs there are and two parameters on each line,
    one for the predicate, one for the object, with a parameter naming following the one of
    @see multi_predicate_object_pairs_parameter_rewriting

    @param name: the name of the MultiPredicateObjectPairs parameter
    @param nb_multi: the number of predicate-object pairs, and therefore of
     duplication of the line where the parameter is located
    @param query_body: the query body where the rewriting of parameter placeholders is
    done
    @return: the rewritten query body
    """
    query_split = query_body.query_string.split("\n")
    to_find = f"${name}"

    index = next((i for i, line in enumerate(query_split) if to_find in line), None)

    if index is None:
        raise MalformedRuleException(
            "Multi predicate object pair parameter found in "
            "specifications, but the parameter name is not found in the query"
        )

    def replacement(nb):
        return f"${name}_{nb}_{'predicate'} ${name}_{nb}_{'object'}"

    new_lines = [query_split[index].replace(to_find, replacement(i)) for i in range(nb_multi)]

    if len(new_lines) == 0:
        del query_split[index]
    else:
        query_split[index] = "\n".join(new_lines)

    query_body_str = "\n".join(query_split)

    return SparqlQueryBody({"query_string": query_body_str})


def has_multi_predicate_object_pairs(
        parameter_spec: List[ParameterSpecification], parameter_values: Dict[str, Any]
) -> Optional[Tuple[int, str, int]]:
    """
    Checks whether within the rule parameters (parameter specification),
    a parameter of type MultiPredicateObjectPair exists.

    @param parameter_spec: the parameter specification of a rule
    @param parameter_values: the parameter values specified by the user
    @return: If a parameter of this type exists, returns the name of the parameter,
        its index within the parameters specifications array, and
        the number of predicate-object pairs specified by the user in the parameter values
        else returns None
    """
    types = [p.type for p in parameter_spec]

    try:
        idx = types.index(ParameterType.MULTI_PREDICATE_OBJECT_PAIR)
        name = parameter_spec[idx].name
        if name not in parameter_values or parameter_values[name] is None:
            return idx, name, 0

        return idx, name, len(parameter_values[name])

    except ValueError:
        pass

    return None


def multi_predicate_object_pairs_parameter_rewriting(
        idx: int,
        parameter_spec: List[ParameterSpecification],
        parameter_values: Dict[str, Any]
) -> Tuple[List[ParameterSpecification], Dict[str, Any]]:
    """
    Predicate-object pairs consist of type-value pairs (pairs of pairs) specified by the user
    of the rule to add additional properties of an entity to retrieve in a SPARQL query's
    WHERE statement.
    They are injected into the query, as such, each predicate and object become
    query parameters.
    These type-value pairs are split into:
    - an entry into the parameter specifications with
         - name: the MultiPredicateObject parameter name concatenated
        with "object" or "predicate" and the object-predicate pair index,
         - type: the type component of the type-value pair
    - an entry into the parameter values with:
        - name: the MultiPredicateObject parameter name concatenated
        with "object" or "predicate" and the object-pair pair index,
        - value: the value component of the type-value pair

    @param idx: The index in the parameter specifications of the parameter of type
    MultiPredicateObjectPair
    @param parameter_spec: the parameter specifications of the rule
    @param parameter_values: the parameter values as specified by the user
    @return: new parameter specifications and values, with the information specified
    by the user in the parameter values appropriately reorganized in the parameter specifications
    and parameter values so that future steps can treat these parameters the way standard
     ParameterType.s are being treated
    """
    spec = parameter_spec[idx]
    del parameter_spec[idx]
    name = spec.name

    if name not in parameter_values:
        return parameter_spec, parameter_values
    provided_value = parameter_values[name]
    del parameter_values[name]

    if provided_value is not None:
        for (i, pair) in enumerate(provided_value):
            ((predicate_value, predicate_type),
             (object_value, object_type)) = pair

            descriptions = ["predicate", "object"]
            values = [predicate_value, object_value]
            types = [predicate_type, object_type]

            for type_, value, description in zip(types, values, descriptions):
                constructed_name = f"{name}_{str(i)}_{description}"
                parameter_spec.append(ParameterSpecification({
                    "type": type_,
                    "name": constructed_name
                }))
                parameter_values[constructed_name] = value

    return parameter_spec, parameter_values


def multi_check(
        query: Query, parameter_values: Dict[str, Any]
) -> Tuple[List[ParameterSpecification], Dict[str, Any]]:
    """
    Checks inside a SparqlQuery whether it supports a parameter of type MultiPredicateObjectPair.
    @see has_multi_predicate_object_pairs
    If so, rewrites the query using the parameter values provided by the user which will
    contain information to be put in the query body.
    @see multi_predicate_object_pairs_query_rewriting
    Then, creates additional entries in the parameter specification,
    @see multi_predicate_object_pairs_parameter_rewriting

    @param query: the query
    @type query: Query
    @param parameter_values: user provided parameter values
    @type parameter_values: Dict[str, Any]
    @return: the updated parameter specifications and parameter values
    @rtype: Tuple[List[ParameterSpecification], Dict[str, Any]]
    """
    multi = has_multi_predicate_object_pairs(query.parameter_specifications, parameter_values)

    if multi:
        if not isinstance(query, SparqlQuery):
            raise InvalidParameterTypeException(
                ParameterType.MULTI_PREDICATE_OBJECT_PAIR, query.type
            )

        (idx, name, nb_multi) = multi
        query.body = multi_predicate_object_pairs_query_rewriting(name, nb_multi, query.body)

        parameter_spec, parameter_values = multi_predicate_object_pairs_parameter_rewriting(
            idx,
            query.parameter_specifications,
            parameter_values or {}
        )

    else:
        parameter_spec = query.parameter_specifications

    return parameter_spec, parameter_values
