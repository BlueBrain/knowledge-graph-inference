class Statistic:
    min: float
    max: float
    std: float
    mean: float
    N: float

    def __init__(self, min_, max_, std_, mean_, N_):
        self.min = min_
        self.max = max_
        self.std = std_
        self.mean = mean_
        self.N = N_

    @staticmethod
    def from_json(obj):
        statistics = dict((el["statistic"], el) for el in obj["series"])

        def _get_value(value_str):
            return statistics[value_str]["value"]

        return Statistic(
            min_=_get_value("min"), max_=_get_value("max"), std_=_get_value("standard deviation"),
            mean_=_get_value("mean"), N_=_get_value("N")
        )

    def to_series(self):
        stats_dict = {
            "min": self.min,
            "max": self.max,
            "mean": self.mean,
            "standard deviation": self.std,
            "N": self.N
        }

        return [{
            "statistic": key,
            "unitCode": "dimensionless",
            "value": val
        } for key, val in stats_dict.items()]
