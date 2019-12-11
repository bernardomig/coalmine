from coalmine import Dataset, register_builder


@register_builder('range')
class RangeBuilder(Dataset):

    def __init__(self, end):
        self.end = end

    def __iter__(self):
        return (i for i in range(self.end))
