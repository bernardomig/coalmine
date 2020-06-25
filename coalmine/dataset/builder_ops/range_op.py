
from coalmine.dataset import Dataset, register_op


@register_op('range', type='staticmethod')
class RangeOp(Dataset):
    """Constructs a new dataset with similar properties as with the `range` operator.

    Args:
        start (int): the first element. Default: 0.
        end (int): the last element.
        step (int): the step size. Default: 1.
    """

    def __init__(self, *args, **kwargs):
        if len(kwargs) == 0:
            if len(args) == 1:
                self.end = args[0]
                self.begin = 0
                self.step = 1
            elif len(args) == 2:
                self.begin = args[0]
                self.end = args[1]
                self.step = 1
            elif len(args) == 3:
                self.begin = args[0]
                self.end = args[1]
                self.step = args[2]
            else:
                raise ValueError('no args specified')
        else:
            if hasattr(kwargs, 'end') and len(args) == 0:
                self.end = kwargs['end']
                self.begin = getattr(kwargs, 'start', 0)
                self.step = getattr(kwargs, 'step', 1)
            elif hasattr(kwargs, 'start') and len(args) == 1:
                self.end = args[0]
                self.begin = kwargs['start']
                self.step = getattr(kwargs, 'step', 1)
            elif len(args) == 2:
                self.end = args[1]
                self.begin = args[0]
                self.step = getattr(kwargs, 'step', 1)
            else:
                raise ValueError('incorrect arguments specified')

        if (self.end - self.begin) * self.step <= 0:
            raise ArithmeticError('the range is infinite')

    def __len__(self):
        return abs((self.end - self.begin) // self.step)

    def __getitem__(self, idx):
        return self.begin + idx * self.step

    def __iter__(self):
        for item in range(self.begin, self.end, self.step):
            yield item
