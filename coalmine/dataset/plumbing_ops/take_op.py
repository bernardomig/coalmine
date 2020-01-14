from coalmine.dataset import Dataset, register_op


@register_op('take')
class TakeOp(Dataset):

    def __init__(self, dataset, num_elements):
        self.dataset = dataset
        self.num_elements = num_elements

    def __len__(self):
        return min(len(self.dataset), self.num_elements)

    def __getitem__(self, idx):
        return self.dataset[idx]

    def __iter__(self):
        for idx, item in enumerate(self.dataset):
            if idx == self.num_elements:
                break
            yield item
