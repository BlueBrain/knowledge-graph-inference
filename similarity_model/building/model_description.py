class ModelDescription:
    def __init__(self, model_dictionary):
        self.name = model_dictionary["name"]
        self.description = model_dictionary["description"]
        self.filename = model_dictionary["filename"]
        self.label = model_dictionary["label"]
        self.distance = model_dictionary["distance"]
        self.model = model_dictionary["model"]
