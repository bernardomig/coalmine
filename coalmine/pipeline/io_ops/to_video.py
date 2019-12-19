
from coalmine.pipeline import register_pipeline_op

import numpy as np
import cv2

from tqdm.auto import tqdm


@register_pipeline_op('to_video')
def to_video_op(pipeline, filename, frame_size, framerate, with_info=True):

    width, height = frame_size

    video = cv2.VideoWriter(filename,
                            cv2.VideoWriter_fourcc(*'MJPG'),
                            framerate,
                            frame_size)

    if video.isOpened() == False:
        raise ValueError("Error opening video for writing")

    for frame in tqdm(pipeline):
        assert type(frame) == np.ndarray
        assert frame.shape[:2] == (height, width)
        video.write(frame)

    video.release()
