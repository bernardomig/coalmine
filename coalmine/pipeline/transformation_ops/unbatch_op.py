from coalmine.pipeline import Pipeline, register_pipeline_op


from math import ceil, floor
import numpy as np


@register_pipeline_op('unbatch')
class UnbatchOp(Pipeline):

    def __init__(self, pipeline, batch_size=None):
        self.pipeline = pipeline
        self.batch_size = batch_size

    def __len__(self):
        if self.batch_size:
            return len(self.pipeline) * self.batch_size
        else:
            raise TypeError(
                "please specify the batch_size in the unbatch"
                "operation in order to calculate the length of the pipeline.")

    def __iter__(self):
        for item in self.pipeline:
            if type(item) == np.ndarray:
                for item in item:
                    yield item
            elif type(item) == tuple:
                for i in range(self.batch_size):
                    yield tuple((t[i] for t in item))
            elif type(item) == dict:
                for i in range(self.batch_size):
                    yield {k: t[i] for k, t in item.items()}
