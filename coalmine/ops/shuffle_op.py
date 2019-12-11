from coalmine import Dataset, register_operation

from random import randrange


@register_operation('shuffle')
class ShuffleOp(Dataset):
    def __init__(self, dataset, shuffle_size):
        self.dataset = dataset
        self.shuffle_size = shuffle_size

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
