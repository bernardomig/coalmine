from coalmine.pipeline import Pipeline, register_pipeline_op


@register_pipeline_op('take')
class TakeOp(Pipeline):

    def __init__(self, pipeline, num_elements):
        self.pipeline = pipeline
        self.num_elements = num_elements

    def __len__(self):
        return min(len(self.pipeline), self.num_elements)

    def __iter__(self):
        for idx, item in enumerate(self.pipeline):
            if idx == self.num_elements:
                break
            yield item
