
from coalmine.pipeline import Pipeline, register_pipeline_op

from random import randrange


@register_pipeline_op('shuffle')
class ShuffleOp(Pipeline):
    def __init__(self, pipeline, shuffle_size):
        self.pipeline = pipeline
        self.shuffle_size = shuffle_size

    def __len__(self):
        return len(self.pipeline)

    def __iter__(self):
        buffer = []

        for item in self.pipeline:
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