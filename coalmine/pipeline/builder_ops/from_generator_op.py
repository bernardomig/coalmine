
from coalmine.pipeline import Pipeline, register_pipeline_op


@register_pipeline_op('from_generator', type='staticmethod')
class FromGeneratorOp(Pipeline):

    def __init__(self, generator_fn, size=None, args=()):
        self.generator_fn = generator_fn
        self.size = size
        self.args = args

    def __len__(self):
        if self.size:
            return self.size
        else:
            raise TypeError(
                "no size specified in FromGenerator Operation.\n"
                "If a len is required, specify it on .from_generator, such as:\n"
                ">>> Pipeline.from_generator(gen_fn, size = 20).")

    def __iter__(self):
        return self.generator_fn(*self.args)
