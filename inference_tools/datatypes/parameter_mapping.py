class ParameterMapping:
    parameter_name: str
    path: str

    def __init__(self, obj):
        self.parameter_name = obj.get("parameterName", None)
        self.path = obj.get("path", None)

    def __repr__(self):
        return f"Parameter Name: {self.parameter_name} ; Path: {self.path}"
