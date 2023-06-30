from kgforge.core import Resource


class ResourceTest(Resource):

    def __init__(self, dict_):
        self.__dict__.update(dict_)