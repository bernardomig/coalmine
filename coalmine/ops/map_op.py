from coalmine import Dataset, register_operation

from threading import Thread, Lock, Event
from queue import Queue, Empty


@register_operation('map')
class MapOp(Dataset):
    def __init__(self, dataset, map_fn, num_parallel_calls=None):
        if num_parallel_calls is not None and num_parallel_calls < 1:
            raise ValueError("num_parallel_calls must be a positive number")

        self.dataset = dataset
        self.map_fn = map_fn
        self.num_parallel_calls = num_parallel_calls

    def __iter__(self):
        if self.num_parallel_calls is None:
            return map(self.map_fn, self.dataset)
        else:
            return parallel_map(
                self.map_fn, self.dataset, num_threads=self.num_parallel_calls)


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


def parallel_map(map_fn, iterable, num_threads):
    if not num_threads > 0:
        raise ValueError("num_threads must be greater than 1")

    in_queue = Queue(num_threads)
    out_queue = Queue(num_threads)
    stop_event = Event()
    stop_workers = Event()
    counter = AtomicCounter()

    def worker():
        while not stop_workers.is_set():
            try:
                item = in_queue.get(timeout=1)
                item = map_fn(item)
                out_queue.put(item)
            except Empty:
                pass

    def publisher():
        for item in iterable:
            counter.increment()
            in_queue.put(item)
        stop_event.set()

    Thread(target=publisher).start()
    for _ in range(num_threads):
        Thread(target=worker).start()

    while (not stop_event.is_set) or counter.current > 0:
        item = out_queue.get()
        yield item
        counter.decrement()

    stop_workers.set()
