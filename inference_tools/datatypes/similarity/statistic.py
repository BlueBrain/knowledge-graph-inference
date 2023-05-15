class Statistic:
    min: float
    max: float
    std: float
    mean: float
    N: int

    def __init__(self, obj):
        statistics = dict((el["statistic"], el) for el in obj["series"])
        self.min = statistics["min"]["value"]
        self.max = statistics["max"]["value"]
        self.std = statistics["standard deviation"]["value"]
        self.mean = statistics["mean"]["value"]
        self.N = statistics["N"]["value"]
