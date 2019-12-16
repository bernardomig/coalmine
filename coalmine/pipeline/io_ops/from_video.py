from coalmine.pipeline import Pipeline, register_pipeline_op

import cv2


@register_pipeline_op('from_video')
class FromVideoOp(Pipeline):

    def __init__(self, filename):
        self.filename = filename

    def __iter__(self):
        video = cv2.VideoCapture(self.filename)
        assert video.isOpened() == True

        while True:
            ret, frame = video.read()
            if ret:
                yield frame
            else:
                break

        video.release()
