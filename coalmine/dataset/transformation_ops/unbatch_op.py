from coalmine.dataset import Dataset, register_op


from math import ceil, floor
import numpy as np


@register_op('unbatch')
class UnbatchOp(Dataset):

    def __init__(self, dataset, batch_size=None):
        self.dataset = dataset
        self.batch_size = batch_size

    def __len__(self):
        if self.batch_size:
            return len(self.dataset) * self.batch_size
        else:
            raise TypeError(
                "please specify the batch_size in the unbatch"
                "operation in order to calculate the length of the dataset.")

    def __iter__(self):
        for item in self.dataset:
            if type(item) == np.ndarray:
                for item in item:
                    yield item
            elif type(item) == tuple:
                for i in range(self.batch_size):
                    yield tuple((t[i] for t in item))
            elif type(item) == dict:
                for i in range(self.batch_size):
                    yield {k: t[i] for k, t in item.items()}
