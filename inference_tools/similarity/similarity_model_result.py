from typing import Dict, Tuple
from dataclasses import dataclass


@dataclass
class SimilarityModelResult:
    id: str
    score: float
    score_breakdown: Dict[str, Tuple[float, float]]

    def to_json(self):
        return {
            "id": self.id,
            "score": self.score,
            "score_breakdown": self.score_breakdown
        }
