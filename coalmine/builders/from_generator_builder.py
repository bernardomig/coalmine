from coalmine import Dataset, register_builder


@register_builder('from_generator')
class FromGeneratorBuilder(Dataset):
    def __init__(self, generator, *args, **kwargs):
        self.generator = generator
        self.args = args
        self.kwargs = kwargs

    def __iter__(self):
        for item in self.generator(*self.args, **self.kwargs):
            yield item
