from coalmine.dataset import Dataset, register_op


@register_op('repeat')
class RepeatOp(Dataset):

    def __init__(self, dataset, num_repeats):
        self.dataset = dataset
        self.num_repeats = num_repeats

    def __len__(self):
        return len(self.dataset) * self.num_repeats

    def __getitem__(self, idx):
        idx = idx % len(self.dataset)
        return self.dataset[idx]

    def __iter__(self):
        for _ in range(self.num_repeats):
            for item in self.dataset:
                yield item
