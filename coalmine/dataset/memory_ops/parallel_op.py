
from coalmine.dataset import Dataset, register_op

from threading import Thread, Event, Lock
from queue import Queue, Empty

from time import sleep


@register_op('parallel')
class ParallelOp(Dataset):

    def __init__(self, dataset, num_parallel_calls, inorder=False):
        if num_parallel_calls < 1:
            raise ValueError("num_parallel_calls has to be greater than 0")

        if not hasattr(dataset, '__getitem__'):
            raise ValueError("dataset has to support random access")

        self.dataset = dataset
        self.num_parallel_calls = num_parallel_calls
        self.inorder = inorder

    def __len__(self):
        return len(self.dataset)

    def __getitem__(self, idx):
        return self.dataset[idx]

    def __iter__(self):
        in_queue = Queue(self.num_parallel_calls)
        out_queue = Queue(self.num_parallel_calls)
        stop_event = Event()
        stop_workers = Event()
        counter = AtomicCounter()
        if self.inorder:
            ticket = AtomicCounter()

        def worker():
            while not stop_workers.is_set():
                try:
                    idx = in_queue.get(timeout=0.01)
                    item = self.dataset[idx]

                    if self.inorder:
                        while ticket.current != idx:
                            sleep(1e-4)
                    out_queue.put(item)
                    if self.inorder:
                        ticket.increment()
                except Empty:
                    pass

        def publisher():
            for idx in range(len(self.dataset)):
                if stop_event.is_set():
                    break
                counter.increment()
                in_queue.put(idx)
            stop_event.set()

        try:
            Thread(target=publisher).start()
            for _ in range(self.num_parallel_calls):
                Thread(target=worker).start()

            while (not stop_event.is_set()) or counter.current > 0:
                item = out_queue.get()
                yield item
                counter.decrement()
        except:
            pass
        finally:
            stop_workers.set()


class AtomicCounter:
    def __init__(self):
        self._value = 0
        self._lock = Lock()

    @property
    def current(self):
        return self._value

    def increment(self):
        with self._lock:
            self._value += 1

    def decrement(self):
        with self._lock:
            self._value -= 1
