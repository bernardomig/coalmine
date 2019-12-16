from coalmine.pipeline import Pipeline, register_pipeline_op

from threading import Thread, Event
from queue import Queue


@register_pipeline_op('prefetch')
class PrefetchOp(Pipeline):

    def __init__(self, pipeline, buffer_size):
        self.pipeline = pipeline
        self.buffer_size = buffer_size

    def __iter__(self):
        queue = Queue(self.buffer_size)
        stop_event = Event()

        def producer():
            for item in self.pipeline:
                queue.put(item)

            stop_event.set()

        producer_thread = Thread(target=producer)
        producer_thread.start()

        while not (stop_event.is_set() and queue.empty()):
            yield queue.get()