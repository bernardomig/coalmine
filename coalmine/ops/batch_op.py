from coalmine import Dataset, register_operation

import numpy as np


class BatchOp(Dataset):

    def __init__(self, dataset, batch_size, drop_last=True, collate_fn=np.stack):
        self.dataset = dataset
        self.batch_size = batch_size
        self.drop_last = drop_last
        self.collate_fn = collate_fn

    def __iter__(self):
        batch = BatchItem()

        for item in self.dataset:
            batch.append(item)
            if len(batch) == self.batch_size:
                yield self.collate_fn(batch.value)
                batch = BatchItem()

        if not self.drop_last and len(batch) > 0:
            yield self.collate_fn(batch)


class BatchItem:
    def __init__(self):
        self.num_element = None
        self.batch_size = 0
        self.batch = None

    def __len__(self):
        return self.batch_size

    @property
    def value(self):
        return self.batch

    def append(self, item):
        if self.batch is None:
            if isinstance(item, tuple):
                self.num_element = len(item)
                self.batch = tuple(([] for _ in range(self.num_element)))
            else:
                self.num_element = 0
                self.batch = []

        if self.num_element == 0:
            self.batch.append(item)
        else:
            for i, el in enumerate(item):
                self.batch[i].append(el)

        self.batch_size += 1
