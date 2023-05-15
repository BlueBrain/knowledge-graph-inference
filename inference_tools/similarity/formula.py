from enum import Enum


class Formula(Enum):
    COSINE = "cosine"
    EUCLIDEAN = "euclidean"
    POINCARE = "poincare"

    def get_formula(self):
        formulas = {
            "cosine":
                "if (doc['embedding'].size() == 0) { return 0; } "
                "double d = cosineSimilarity(params.query_vector, doc.embedding); "
                "return (d + 1.0) / 2",  # d ranges between 0 and 1
            "euclidean":
                "if (doc['embedding'].size() == 0) { return 0; } "
                "double d = l2norm(params.query_vector, doc.embedding); "
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
                "return 1 / (1 + d);"  # from distance to similarity
        }
        return formulas[self.value]
