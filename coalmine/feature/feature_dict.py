
class FeatureDict:

    def __init__(self, features):
        self.features = features

    def __call__(self, input):
        assert type(input) == dict

        return {
            key: self.features[key](value)
            for key, value in input.items()
        }
