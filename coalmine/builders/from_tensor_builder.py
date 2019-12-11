
from coalmine import Dataset, register_builder

import numpy as np


@register_builder('from_tensors')
class FromTensorsBuilder(Dataset):

    def __init__(self, tensors):
        tensors, size, shapes, dtypes = infer_tensor(tensors)
        self.tensors = tensors
        self.size = size
        self.output_shapes = shapes
        self.dtypes = dtypes

    def __iter__(self):
        if type(self.tensors) == np.ndarray:
            for tensor in self.tensors:
                yield tensor
        elif type(self.tensors) == tuple:
            for i in range(self.size):
                yield tuple((el[i] for el in self.tensors))
        elif type(self.tensors) == dict:
            for i in range(self.size):
                yield {key: tensor[i] for key, tensor in self.tensors.items()}


def infer_tensor(tensor):
    if type(tensor) == np.ndarray:
        return tensor, len(tensor), tensor.shape[1:], tensor.dtype
    elif type(tensor) == tuple:
        if len(tensor) == 0:
            raise ValueError(
                'tuple of tensors should have at least one element')
        tensors, sizes, shape, dtype = zip(
            *(infer_tensor(inner) for inner in tensor))
        size = sizes[0]
        if not all((size == inner_size for inner_size in sizes)):
            raise ValueError(
                'tuple of tensors should have all the same size')
        return tensors, size, shape, dtype
    elif type(tensor) == list:
        cast = np.array(tensor)
        return infer_tensor(cast)
    elif type(tensor) == dict:
        types = {key: infer_tensor(t) for key, t in tensor.items()}
        sizes = [size for _, (_, size, _, _) in types.items()]
        size = sizes[0]
        if not all((size == inner for inner in sizes)):
            raise ValueError('all tensors in `dict` should have the same size')

        tensors = {key: tensor for key, (tensor, _, _, _) in types.items()}
        shape = {key: shape for key, (_, _, shape, _) in types.items()}
        dtypes = {key: dtype for key, (_, _, _, dtype) in types.items()}

        return tensors, size, shape, dtypes
    else:
        raise ValueError('input tensor is not a tensor type')
