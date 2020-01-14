
from coalmine.dataset import Dataset, register_op

from random import randrange


@register_op('shuffle')
class ShuffleOp(Dataset):
    def __init__(self, dataset, shuffle_size):
        self.dataset = dataset
        self.shuffle_size = shuffle_size

    def __len__(self):
        return len(self.dataset)

    def __getitem__(self, idx):
        return self.dataset[idx]

    def __iter__(self):
        buffer = []

        for item in self.dataset:
            if len(buffer) < self.shuffle_size:
                # fill the buffer
                buffer.append(item)
                continue
            # get a random item from buffer
            idx = randrange(len(buffer))
            yield buffer[idx]
            # replace the last element with the new item
            buffer[idx] = item

        while len(buffer) > 0:
            idx = randrange(len(buffer))
            yield buffer.pop(idx)
