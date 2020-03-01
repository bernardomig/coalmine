
from coalmine.dataset import Dataset, register_op

import numpy as np
import torch


@register_op('from_tensors', type='staticmethod')
class FromTensorsOp(Dataset):

    def __init__(self, tensors):
        if type(tensors) == tuple:
            if len(tensors) == 0:
                raise ValueError('tuple must have at least one tensor')
            self.size = len(tensors[0])
            if not all((self.size == len(t) for t in tensors)):
                raise ValueError('all tensors should have the same size')
            self.tensors = tuple(map(convert_to_tensor, tensors))
        elif type(tensors) == dict:
            if len(tensors) == 0:
                raise ValueError('dict must have at least one tensor')
            sizes = [len(inner) for inner in tensors.values()]
            self.size = sizes[0]
            if not all((self.size == len(t) for t in tensors.values())):
                raise ValueError('all tensors should have the same size')
            self.tensors = {key: convert_to_tensor(tensor)
                            for key, tensor in tensors.items()}
        else:
            self.tensors = convert_to_tensor(tensors)
            self.size = len(self.tensors)

    def __len__(self):
        return self.size

    def __getitem__(self, idx):
        if type(self.tensors) == tuple:
            return tuple((t[idx] for t in self.tensors))
        elif type(self.tensors) == dict:
            return {k: t[idx] for k, t in self.tensors.items()}
        else:
            return self.tensors[idx]

    def __iter__(self):
        if type(self.tensors) == np.ndarray:
            for item in self.tensors:
                yield item
        elif type(self.tensors) == tuple:
            for i in range(self.size):
                yield tuple((t[i] for t in self.tensors))
        elif type(self.tensors) == dict:
            for i in range(self.size):
                yield {k: t[i] for k, t in self.tensors.items()}


def convert_to_tensor(arr):
    if type(arr) == list:
        return torch.tensor(arr)
    elif type(arr) == np.ndarray:
        return torch.from_numpy(arr)
    elif type(arr) == torch.Tensor:
        return arr
    else:
        raise ValueError("unknown tensor type")
