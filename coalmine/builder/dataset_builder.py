
from coalmine.dataset import Dataset


class DatasetBuilder:

    def __init__(self, root_dir):
        self.config = {
            'root_dir': root_dir
        }

    def _prepare(self, config):
        raise NotImplementedError()

    def _generate_examples(self):
        raise NotImplementedError()

    def build(self, split='train'):
        ds = self._prepare(self.config)
        ds = ds[split]

        examples = list(self._generate_examples(**ds))

        return (
            Dataset
            .from_tensors(examples))
