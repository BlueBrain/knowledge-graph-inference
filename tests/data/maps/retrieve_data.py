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

from kgforge.core.wrappings.dict import DictWrapper

from tests.data.maps.id_data import make_model_id, revify

retrieve_map = {
    f"{make_model_id(1)}{revify(17)}": DictWrapper({"similarity": "euclidean"}),
    f"{make_model_id(2)}{revify(17)}": DictWrapper({"similarity": "euclidean"})
}
