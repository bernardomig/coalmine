from pathlib import Path

import numpy as np
import cv2


class ImageFeature:

    def __init__(self, grayscale=False):

        self.grayscale = grayscale

    def __call__(self, input):
        if type(input) == np.ndarray:
            return input
        elif type(input) == str or str(input) == Path:
            cv2_read_flags = (
                cv2.IMREAD_GRAYSCALE
                if self.grayscale
                else cv2.IMREAD_COLOR)

            img = cv2.imread(str(input), cv2_read_flags)
            assert img is not None

            if not self.grayscale:
                img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

            return img
