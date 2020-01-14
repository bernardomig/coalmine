
from coalmine.dataset import Dataset, register_op


@register_op('from_generator', type='staticmethod')
class FromGeneratorOp(Dataset):

    def __init__(self, generator_fn, len=None, args=()):
        self.generator_fn = generator_fn
        self.len = len
        self.args = args

    def __len__(self):
        if self.len:
            return self.len
        else:
            raise TypeError(
                "no size specified in FromGenerator Operation.\n"
                "If a len is required, specify it on .from_generator, such as:\n"
                ">>> Dataset.from_generator(gen_fn, len=20).")

    def __iter__(self):
        return self.generator_fn(*self.args)
