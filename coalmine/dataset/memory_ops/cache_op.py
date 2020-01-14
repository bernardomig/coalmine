
from coalmine.dataset import Dataset, register_op


@register_op('cache')
class CacheOp(Dataset):

    def __init__(self, dataset):
        self.dataset = dataset
        self.cache = None
        self.valid = False

    def __len__(self):
        return (
            len(self.cache)
            if self.valid
            else len(self.dataset))

    def __getitem__(self, idx):
        if self.valid:
            return self.cache[idx]
        else:
            raise ValueError("")

    def __iter__(self):
        if self.valid:
            for item in self.cache:
                yield item
        else:
            self.cache = []
            for item in self.dataset:
                self.cache.append(item)
                yield item
