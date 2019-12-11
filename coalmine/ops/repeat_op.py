
from coalmine import Dataset, register_operation


@register_operation('repeat')
class RepeatOp(Dataset):

    def __init__(self, dataset,  num_repeats):
        self.dataset = dataset
        self.num_repeats = num_repeats

    def __iter__(self):
        for _ in range(self.num_repeats):
            for item in self.dataset:
                yield item
