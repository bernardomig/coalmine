from coalmine import Dataset, register_builder


@register_builder('zip')
class ZipOp(Dataset):

    def __init__(self, datasets):
        if type(datasets) is not tuple:
            raise ValueError('datasets should be a tuple of Datasets')

        self.datasets = datasets

    def __iter__(self):
        return zip(*self.datasets)
