from coalmine.dataset import Dataset, register_op


@register_op('enumerate')
class EnumerateOp(Dataset):

    def __init__(self, dataset):
        self.dataset = dataset

    def __len__(self):
        return len(self.dataset)

    def __getitem__(self, idx):
        return idx, self.dataset[idx]

    def __iter__(self):
        return enumerate(self.dataset)
