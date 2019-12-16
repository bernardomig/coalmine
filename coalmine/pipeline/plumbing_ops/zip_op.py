from coalmine.pipeline import Pipeline, register_pipeline_op


@register_pipeline_op('zip', type='staticmethod')
class ZipOp(Pipeline):

    def __init__(self, pipelines):
        if type(pipelines) is not tuple:
            raise ValueError('datasets should be a tuple of Datasets')

        self.pipelines = pipelines

    def __len__(self):
        return len(self.pipelines[0])

    def __iter__(self):
        return zip(*self.pipelines)
