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

from typing import Dict


class Statistic:
    min: float
    max: float
    std: float
    mean: float
    count: float

    def __init__(self, min_, max_, std_, mean_, count_):
        self.min = min_
        self.max = max_
        self.std = std_
        self.mean = mean_
        self.count = count_

    @staticmethod
    def from_json(obj: Dict) -> 'Statistic':
        """
        Builds an instance of this class from a dictionary
        @param obj: the dictionary
        @type obj: Dict
        @return: an instance of this class
        @rtype: Statistic
        """
        statistics = dict((el["statistic"], el) for el in obj["series"])

        def _get_value(value_str):
            return statistics[value_str]["value"]

        return Statistic(
            min_=_get_value("min"), max_=_get_value("max"), std_=_get_value("standard deviation"),
            mean_=_get_value("mean"), count_=_get_value("N")
        )
