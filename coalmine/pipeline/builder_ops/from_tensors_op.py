
from coalmine.pipeline import Pipeline, register_pipeline_op

import numpy as np


@register_pipeline_op('from_tensors', type='staticmethod')
class FromTensorsOp(Pipeline):

    def __init__(self, tensors):
        if type(tensors) == list:
            self.tensors = np.array(tensors)
            self.size = len(self.tensors)
        elif type(tensors) == np.ndarray:
            self.tensors = np.array(tensors)
            self.size = len(self.tensors)
        elif type(tensors) == tuple:
            if len(tensors) == 0:
                raise ValueError('tuple must have at least one tensor')
            self.size = len(tensors[0])
            if not all((self.size == len(t) for t in tensors)):
                raise ValueError('all tensors should have the same size')
            self.tensors = tuple(map(np.array, tensors))
        elif type(tensors) == dict:
            if len(tensors) == 0:
                raise ValueError('dict must have at least one tensor')
            sizes = [len(inner) for inner in tensors.values()]
            self.size = sizes[0]
            if not all((self.size == len(t) for t in tensors.values())):
                raise ValueError('all tensors should have the same size')
            self.tensors = {key: np.array(tensor)
                            for key, tensor in tensors.items()}
        else:
            raise ValueError('incompatible tensor type')

    def __len__(self):
        return self.size

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
