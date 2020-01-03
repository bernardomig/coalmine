

class FeatureCompose:

    def __init__(self, features):
        self.features = features

    def __call__(self, input):
        for feature in self.features:
            input = feature(input)
        return input
