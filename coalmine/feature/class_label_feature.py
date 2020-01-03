
import numpy as np


class ClassLabelFeature:

    def __init__(self, classes, train_classes=None, ignore_id=255):
        self.num_classes = (
            len(classes)
            if type(classes) in {list, np.ndarray}
            else classes
        )

        if type(classes) in {list, np.ndarray}:
            self.labels = classes

        self.train_classes = train_classes

        if train_classes is not None:
            self.mapping = np.zeros(255, dtype=np.uint8) + ignore_id
            idx = 0
            for i, c in enumerate(classes):
                if c in train_classes:
                    self.mapping[i] = idx
                    idx += 1
                else:
                    self.mapping[i] = ignore_id

    def __call__(self, input):
        if self.train_classes is not None:
            return self.mapping[input]
        else:
            return input
