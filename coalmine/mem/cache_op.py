from coalmine import Dataset, register_operation


@register_operation('cache')
class CacheOp(Dataset):

    def __init__(self, dataset):
        self.dataset = dataset
        self.cache = None

    def __iter__(self):
        if self.cache is not None:
            for item in self.cache:
                yield item
        else:
            self.cache = []
            for item in self.dataset:
                self.cache.append(item)
                yield item
