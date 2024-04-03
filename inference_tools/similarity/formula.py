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

from enum import Enum


class Formula(Enum):
    COSINE = "cosine"
    EUCLIDEAN = "euclidean"
    POINCARE = "poincare"
    CUSTOM_TMD = "custom_tmd"

    def get_formula(self) -> str:
        """
        Returns the formula to be used in the script score query of a similarity search-based query.
        @see https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-script-score-query.html
        """

        formulas = {
            "cosine":
                "if (doc['embedding'].size() == 0) { return 0; } "
                "double d = cosineSimilarity(params.query_vector, 'embedding'); "
                "return (d + 1.0) / 2",  # d ranges between 0 and 1
            "euclidean":
                "if (doc['embedding'].size() == 0) { return 0; } "
                "double d = l2norm(params.query_vector, 'embedding'); "
                "return (1 / (1 + d))",  # from distance to similarity
            "poincare":
                "if (doc['embedding'].size() == 0) { return 0; } "
                "float[] v = doc['embedding'].vectorValue; "
                "double am = doc['embedding'].magnitude; "
                "double bm = 0; "
                "double dist = 0; "

                "for (int i = 0; i < v.length; i++) { "
                "   bm += Math.pow(params.query_vector[i], 2); "
                "   dist += Math.pow(v[i] - params.query_vector[i], 2); "
                "} "

                "bm = Math.sqrt(bm); "
                "dist = Math.sqrt(dist); "

                "double x = 1 + (2 * Math.pow(dist, 2)) / "
                "   ( (1 - Math.pow(bm, 2)) * (1 - Math.pow(am, 2)) ); "

                "double d = Math.log(x + Math.sqrt(Math.pow(x, 2) - 1)); "
                "return 1 / (1 + d);",  # from distance to similarity
            "custom_tmd": """
                float[] toFloat(byte[] arr) {

                    int length = arr.length/4;

                    float[] vector = new float[length];

                    for (int i = 0; i < length; ++i) {
                        def n = i*4;
                        vector[i] = Float.intBitsToFloat( (arr[n+3] << 24) | ((arr[n+2] & 255) << 16) |  ((arr[n+1] & 255) << 8) |  (arr[n] & 255) );
                    }

                    return vector;
                }

                float[] vector = toFloat(doc["embedding"].value.bytes);

                float[] q_vector = toFloat(Base64.getDecoder().decode(params.query_vector));

                float distance = 0;

                for (int i = 0; i < q_vector.length; ++i) {
                    distance += Math.abs(vector[i] - q_vector[i]);
                }

                return 1/(1+distance);
            """
        }
        return formulas[self.value]
