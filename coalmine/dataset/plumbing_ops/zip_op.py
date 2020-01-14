from coalmine.dataset import Dataset, register_op


@register_op('zip', type='staticmethod')
class ZipOp(Dataset):

    def __init__(self, datasets):
        if type(datasets) is not tuple:
            raise ValueError('datasets should be a tuple of Datasets')

        self.datasets = datasets

    def __len__(self):
        return len(self.datasets[0])

    def __getitem__(self, idx):
        return zip((d[idx] for d in self.datasets))

    def __iter__(self):
        return zip(*self.datasets)
