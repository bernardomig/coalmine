from coalmine.pipeline import Pipeline, register_pipeline_op

import numpy as np
import torch


@register_pipeline_op('to_torch')
class ToTorchOp(Pipeline):

    def __init__(self, pipeline):
        self.pipeline = pipeline

    def __len__(self):
        return len(self.pipeline)

    def __iter__(self):
        for item in self.pipeline:
            if type(item) == np.ndarray:
                yield torch.from_numpy(item)
            elif type(item) == tuple:
                yield tuple(map(torch.from_numpy, item))
            elif type(item) == dict:
                yield {k: torch.from_numpy(t) for k, t in items.items()}
            else:
                raise ValueError('invalid item type')
