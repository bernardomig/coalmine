
from coalmine.pipeline import Pipeline, register_pipeline_op


@register_pipeline_op('cache')
class CacheOp(Pipeline):

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.cache = None
        self.valid = False

    def __len__(self):
        return (
            len(self.cache)
            if self.valid
            else len(self.pipeline))

    def __iter__(self):
        if self.valid:
            for item in self.cache:
                yield item
        else:
            self.cache = []
            for item in self.pipeline:
                self.cache.append(item)
                yield item
