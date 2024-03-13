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

from typing import Any, List, Union, Optional
import json

from kgforge.core import KnowledgeGraphForge

from inference_tools.exceptions.malformed_rule import InvalidParameterTypeException
from inference_tools.helper_functions import _enforce_unique
from inference_tools.exceptions.exceptions import (
    UnsupportedTypeException,
    InvalidValueException,
    ObjectTypeStr, InferenceToolsException
)
from inference_tools.helper_functions import _enforce_list
from inference_tools.nexus_utils.forge_utils import ForgeUtils
from inference_tools.type import ParameterType, PremiseType, QueryType, parameter_list_types


class ParameterFormatter:
    """
    Formatter of input parameter value
    """

    def __init__(self, expand_uri: bool, format_string: Optional[str],
                 join_string: Optional[str] = None, wrap_string: Optional[str] = None):
        """
        @param format_string: the string that will format each individual value
        @type format_string: Optional[str]
        @param join_string: the string that will join all elements of the list of values
        @type join_string: Optional[str]
        @param wrap_string: the string that will wrap around the joined list of values
        @type wrap_string: Optional[str]
        @param expand_uri: whether the individual values are uris that need to be expanded or not
        @type expand_uri: bool
        """
        self.expand_uri: bool = expand_uri
        self.format_string: Optional[str] = format_string
        self.join_string: Optional[str] = join_string
        self.wrap_string: Optional[str] = wrap_string

    def format_list_value(self, value: List[str], forge: KnowledgeGraphForge) -> str:
        """
        Formats the value provided by the user for the input parameter, the value being a list.
        Each individual item is being optionally expanded as an uri, then formatted by a
        format string, then joined by a join string, then wrapped by a wrap string.

        @param forge: a forge instance to expand uris if expand_uri is True
        @type forge: KnowledgeGraphForge
        @param value: a user specified value for the parameter
        @type value: List[str]
        @return: the user value formatted
        @rtype: str
        """

        if self.join_string is None and self.wrap_string is None:
            raise InferenceToolsException(
                "Cannot turn a list into a string without a join string or a wrap string"
            )
        final_value = self.format_value(value=value, forge=forge)

        if self.join_string is not None:
            final_value = self.join_string.join(final_value)

        if self.wrap_string is not None:
            final_value = self.wrap_string.format(final_value)

        return final_value  # type: ignore

    def format_value(
            self, value: Union[str, List[str]], forge: KnowledgeGraphForge
    ) -> Union[str, List[str]]:
        """
        Formats a value by optionally expanding it as an uri, and then applying a format string.
        If multiple values are specified, this logic is applied to all of them.

        @param forge: a forge instance, to expand uris if necessary
        @type forge: KnowledgeGraphForge
        @param value: a value or a list of values to format
        @type value: Union[str, List[str]]
        @return: the user value formatted
        @rtype: Union[str, List[str]]
        """

        def format_singular(x):
            if self.expand_uri:
                x = ForgeUtils.expand_uri(forge, x)
            return self.format_string.format(x) if self.format_string is not None else x

        return [format_singular(el) for el in value] \
            if isinstance(value, list) else format_singular(value)

    @staticmethod
    def format_parameter(
            parameter_type: ParameterType, provided_value: Any,
            query_type: Union[QueryType, PremiseType], forge: KnowledgeGraphForge
    ) -> str:
        """
        Formats the user provided value following the formatting associated with the parameter type
        @param parameter_type: the type of the parameter whose user input value is being formatted
        @type parameter_type: ParameterType
        @param provided_value: the user input value for the parameter
        @type provided_value: Any
        @param query_type: the query type of the query where the parameter is specified
        @type query_type: QueryType
        @param forge: a forge instance to expand uris if the parameter type calls for it
        @type forge: KnowledgeGraphForge
        @return: the formatted parameter value
        @rtype: str
        """

        sparql_types = [
            PremiseType.SPARQL_PREMISE,
            QueryType.SPARQL_QUERY
        ]

        list_formatters = {
            ParameterType.LIST: ParameterFormatter(expand_uri=False, format_string="\"{}\"",
                                                   join_string=", ", wrap_string=None),
            ParameterType.URI_LIST: ParameterFormatter(expand_uri=True, format_string="<{}>",
                                                       join_string=", ", wrap_string=None),
            ParameterType.SPARQL_LIST: ParameterFormatter(expand_uri=False, format_string="<{}>",
                                                          join_string=", ", wrap_string="({})"),

            ParameterType.SPARQL_VALUE_LIST: ParameterFormatter(expand_uri=False,
                                                                format_string="(\"{}\")",
                                                                join_string="\n",
                                                                wrap_string=None),

            ParameterType.SPARQL_VALUE_URI_LIST: ParameterFormatter(expand_uri=True,
                                                                    format_string="(<{}>)",
                                                                    join_string="\n",
                                                                    wrap_string=None)
        }

        if parameter_type in parameter_list_types:

            provided_value = _enforce_list(provided_value)

            if parameter_type == ParameterType.URI_LIST and \
                    query_type in [QueryType.SIMILARITY_QUERY, QueryType.ELASTIC_SEARCH_QUERY,
                                   PremiseType.ELASTIC_SEARCH_PREMISE]:
                return json.dumps(provided_value)

            formatter = list_formatters.get(parameter_type, None)

            if formatter is None:
                raise UnsupportedTypeException(parameter_type, "parameter type")

            if parameter_type in (ParameterType.SPARQL_LIST, ParameterType.SPARQL_VALUE_LIST,
                                  ParameterType.SPARQL_VALUE_URI_LIST) and query_type not in \
                    sparql_types:
                raise InvalidParameterTypeException(parameter_type, query_type)

            return formatter.format_list_value(value=provided_value, forge=forge)

        if parameter_type == ParameterType.BOOL:
            value = _enforce_unique(provided_value).lower()
            if value not in ["true", "false"]:
                raise InvalidValueException(attribute=ObjectTypeStr.PARAMETER.value, value=value,
                                            rest="for an input parameter of type bool")
            return value

        value_formatters = {
            ParameterType.STR: ParameterFormatter(expand_uri=False, format_string="\"{}\""),
            ParameterType.PATH: ParameterFormatter(expand_uri=False, format_string=None),
            ParameterType.QUERY_BLOCK: ParameterFormatter(expand_uri=False, format_string=None),
            ParameterType.URI: ParameterFormatter(expand_uri=True, format_string=None),
            # TODO: figure out if we need to expand uris
            #  when doing ElasticSearch queries
            #  (hard to say in general because it depends on the indexing)
        }

        formatter = value_formatters.get(parameter_type, None)

        if formatter is None:
            raise UnsupportedTypeException(parameter_type, "parameter type")

        value = _enforce_unique(provided_value)

        return formatter.format_value(value=value, forge=forge)  # type: ignore
