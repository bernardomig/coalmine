
from coalmine.pipeline import Pipeline, register_pipeline_op

import os
import glob


@register_pipeline_op('list_files', type='staticmethod')
class ListFilesOp(Pipeline):

    def __init__(self, directory=None, glob=None, extension=None, cache=True, sorted=True):
        if (directory and glob):
            raise ValueError(
                "must only specify either directory or glob, not both.")
        if not (directory or glob):
            raise ValueError(
                "must specify either directory or glob.")

        self.directory = directory
        self.glob = glob

        self.extension = extension
        self.sorted = sorted

        if cache:
            self.files = list(self._find_files())
        else:
            self.files = None

    def __len__(self):
        if self.files:
            return len(self.files)
        else:
            raise TypeError(
                "file list is not caches, therefore the length is unknown."
                "to cache the file list, either pass the parameter cache=True"
                "  or call .cache(), on the pipeline.")

    def _find_files(self):
        if self.directory is not None:
            files = os.listdir(self.directory)
        else:
            files = glob.glob(self.glob)

        if self.sorted:
            files = sorted(files)

        for file in files:
            if os.path.isdir(file):
                continue

            if self.extension:
                _, ext = os.path.splitext(file)
                if ext != self.extension:
                    continue

            if self.directory:
                file = os.path.join(self.directory, file)

            yield file

    def __iter__(self):
        if self.files:
            return iter(self.files)
        else:
            return self._find_files()
