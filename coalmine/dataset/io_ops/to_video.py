
from coalmine.dataset import Dataset, register_op

import numpy as np
import cv2

from tqdm.auto import tqdm


@register_op('to_video')
def to_video_op(dataset, filename, frame_size, framerate, encoding='MJPG', with_info=True):

    width, height = frame_size

    video = cv2.VideoWriter(filename,
                            cv2.VideoWriter_fourcc(*encoding),
                            framerate,
                            frame_size)

    if video.isOpened() == False:
        raise ValueError("Error opening video for writing")

    for frame in tqdm(dataset):
        assert type(frame) == np.ndarray
        assert frame.shape[:2] == (height, width)
        video.write(frame)

    video.release()
