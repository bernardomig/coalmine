
from coalmine.pipeline import Pipeline, register_pipeline_op

#
# TODO: make this multithreaded
#
@register_pipeline_op('filter')
class FilterOp(Pipeline):
    def __init__(self, pipeline, filter_fn):
        self.pipeline = pipeline
        self.filter_fn = filter_fn

    def __iter__(self):
        return filter(self.filter_fn, self.pipeline)
