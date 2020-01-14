
from coalmine.dataset import Dataset, register_op

#
# TODO: make this multithreaded
#
@register_op('filter')
class FilterOp(Dataset):
    def __init__(self, dataset, filter_fn):
        self.dataset = dataset
        self.filter_fn = filter_fn

    def __iter__(self):
        return filter(self.filter_fn, self.dataset)
