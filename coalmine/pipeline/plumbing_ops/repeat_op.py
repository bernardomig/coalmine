from coalmine.pipeline import Pipeline, register_pipeline_op


@register_pipeline_op('repeat')
class RepeatOp(Pipeline):

    def __init__(self, pipeline, num_repeats):
        self.pipeline = pipeline
        self.num_repeats = num_repeats

    def __len__(self):
        return len(self.pipeline) * self.num_repeats

    def __iter__(self):
        for _ in range(self.num_repeats):
            for item in self.pipeline:
                yield item
