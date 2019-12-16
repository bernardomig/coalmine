from coalmine.pipeline import Pipeline, register_pipeline_op


@register_pipeline_op('enumerate')
class EnumerateOp(Pipeline):

    def __init__(self, pipeline):
        self.pipeline = pipeline

    def __len__(self):
        return len(self.pipeline)

    def __iter__(self):
        return enumerate(self.pipeline)
