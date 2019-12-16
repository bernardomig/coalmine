from coalmine.pipeline import Pipeline, register_pipeline_op

from math import ceil, floor
import numpy as np


@register_pipeline_op('batch', type='method')
class BatchOp(Pipeline):

    def __init__(self, pipeline, batch_size, drop_remainder=True):
        self.pipeline = pipeline
        self.batch_size = batch_size
        self.drop_remainder = drop_remainder

    def __len__(self):
        if self.batch_size:
            size = len(self.pipeline) / self.batch_size
            size = floor(size) if self.drop_last else ceil(size)
            return size
        else:
            raise TypeError(
                "please specify the batch_size in the unbatch"
                "operation in order to calculate the length of the pipeline.")

    def __iter__(self):
        batch = None
        batch_size = 0

        for item in self.pipeline:
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
                    yield np.array(batch)
                elif type(batch) == tuple:
                    yield tuple(map(np.array, batch))
                elif type(batch) == dict:
                    yield {k: np.array(t) for k, t in batch.items()}
                batch = None
                batch_size = 0

        if not self.drop_remainder and batch_size > 0:
            if type(batch) == list:
                yield np.array(batch)
            elif type(batch) == tuple:
                yield tuple(map(np.array, batch))
            elif type(batch) == dict:
                yield {k: np.array(t) for k, t in batch.items()}
