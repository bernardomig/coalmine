from coalmine.dataset import Dataset, register_op

from threading import Thread, Event
from queue import Queue


@register_op('prefetch')
class PrefetchOp(Dataset):

    def __init__(self, dataset, buffer_size):
        self.dataset = dataset
        self.buffer_size = buffer_size

    def __len__(self):
        return len(self.dataset)

    def __getitem__(self, idx):
        return self.dataset[idx]

    def __iter__(self):
        queue = Queue(self.buffer_size)
        stop_event = Event()

        def producer():
            for item in self.dataset:
                if stop_event.is_set():
                    break
                queue.put(item)

            stop_event.set()

        try:
            producer_thread = Thread(target=producer)
            producer_thread.start()

            while not (stop_event.is_set() and queue.empty()):
                yield queue.get()

        except:
            pass
        finally:
            stop_event.set()
