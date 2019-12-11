
from coalmine import Dataset, register_builder

import os


@register_builder('list_files')
class ListFilesBuilder(Dataset):

    def __init__(self, path, filter_extension=None):
        self.path = self.path
        self.filter_extension = filter_extension

    def __iter__(self):
        for file in os.listdir(self.path):
            if os.path.isdir(file):
                continue

            if self.filter_extension:
                _, ext = os.path.splitext(file)
                if ext != self.filter_extension:
                    continue

            yield os.path.join(self.path, file)
