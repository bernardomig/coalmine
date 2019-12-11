from coalmine import Dataset, register_operation

from threading import Thread, Event
from queue import Queue


@register_operation('prefetch')
class PrefetchOp(Dataset):

    def __init__(self, dataset, buffer_size):
        self.dataset = dataset
        self.buffer_size = buffer_size

    def __iter__(self):
        queue = Queue(self.buffer_size)
        stop_event = Event()

        def producer():
            for item in self.dataset:
                queue.put(item)

            stop_event.set()

        producer_thread = Thread(target=producer)
        producer_thread.start()

        while not (stop_event.is_set() and queue.empty()):
            yield queue.get()
