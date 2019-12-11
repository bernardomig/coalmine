from coalmine import Dataset, register_operation


@register_operation('enumerate')
class EnumerateOp(Dataset):

    def __init__(self, dataset):
        self.dataset = dataset

    def __iter__(self):
        return enumerate(self.dataset)
