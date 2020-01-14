from coalmine.dataset import Dataset, register_op

from math import ceil, floor
import numpy as np

import torch

# TODO: add tensor awareness


@register_op('batch', type='method')
class BatchOp(Dataset):

    def __init__(self, dataset, batch_size, drop_last=True, collate_fn=None):
        self.dataset = dataset
        self.batch_size = batch_size
        self.drop_last = drop_last
        self.collate_fn = collate_fn

    def __len__(self):
        if self.batch_size:
            size = len(self.dataset) / self.batch_size
            size = floor(size) if self.drop_last else ceil(size)
            return size
        else:
            raise TypeError(
                "please specify the batch_size in the unbatch"
                "operation in order to calculate the length of the dataset.")

    def __iter__(self):
        batch = None
        batch_size = 0

        collate_fn = self.collate_fn
        if collate_fn is None:
            collate_fn = default_collate_fn

        for item in self.dataset:
            if batch == None:
                if type(item) == np.ndarray:
                    batch = []
                elif type(item) == tuple:
                    batch = tuple(([] for i in range(len(item))))
                elif type(item) == dict:
                    batch = {k: [] for k in item.keys()}
                else:
                    raise ValueError('incompatible item type')

            if type(item) == np.ndarray:
                batch.append(item)
            elif type(item) == tuple:
                for i, t in enumerate(item):
                    batch[i].append(t)
            elif type(item) == dict:
                for k, t in item.items():
                    batch[k].append(t)

            batch_size += 1

            if batch_size == self.batch_size:
                if type(batch) == list:
                    yield collate_fn(batch)
                elif type(batch) == tuple:
                    yield tuple(map(collate_fn, batch))
                elif type(batch) == dict:
                    yield {k: collate_fn(t) for k, t in batch.items()}
                batch = None
                batch_size = 0

        if not self.drop_last and batch_size > 0:
            if type(batch) == list:
                yield collate_fn(batch)
            elif type(batch) == tuple:
                yield tuple(map(collate_fn, batch))
            elif type(batch) == dict:
                yield {k: collate_fn(t) for k, t in batch.items()}


def default_collate_fn(item):
    if type(item) == np.ndarray:
        return np.stack(item)
    elif type(item) == torch.Tensor:
        return torch.stack(item)
    else:
        raise ValueError(
            "item type for batches have to be"
            " either torch.Tensor or numpy arrays")
