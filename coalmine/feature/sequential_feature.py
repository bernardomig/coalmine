

class SequentialFeature:

    def __init__(self, feature):
        self.feature = feature

    def __call__(self, input):
        return [self.feature(x) for x in input]
