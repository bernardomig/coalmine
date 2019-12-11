from coalmine import Dataset, register_operation


@register_operation('take')
class TakeOp(Dataset):

    def __init__(self, dataset, num_elements):
        self.dataset = dataset
        self.num_elements = num_elements

    def __iter__(self):
        count = self.num_elements
        for item in self.dataset:
            if count == 0:
                break
            yield item
            count -= 1
