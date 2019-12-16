
from coalmine.pipeline import Pipeline, register_pipeline_op

from threading import Thread, Event, Lock
from queue import Queue, Empty

from time import sleep


@register_pipeline_op('map')
class MapOp(Pipeline):

    def __init__(self, pipeline, map_fn, num_parallel_calls=None, in_order=False):
        self.pipeline = pipeline
        self.map_fn = map_fn

        if num_parallel_calls is not None and num_parallel_calls <= 0:
            raise ValueError("num_parallel_calls has to a positive number")

        self.num_parallel_calls = num_parallel_calls

        self.in_order = in_order

    def __len__(self):
        return len(self.pipeline)

    def __iter__(self):
        if self.num_parallel_calls:
            return parallel_map(self.map_fn, self.pipeline, num_threads=self.num_parallel_calls, inorder=self.in_order)
        else:
            return map(self.map_fn, self.pipeline)


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


def parallel_map(map_fn, iterable, num_threads, inorder=False):
    if not num_threads > 0:
        raise ValueError("num_threads must be greater than 1")

    in_queue = Queue(num_threads)
    out_queue = Queue(num_threads)
    stop_event = Event()
    stop_workers = Event()
    counter = AtomicCounter()
    if inorder:
        ticket = AtomicCounter()

    def worker():
        while not stop_workers.is_set():
            try:
                idx, item = in_queue.get(timeout=0.01)
                item = map_fn(item)

                if inorder:
                    while ticket.current != idx:
                        sleep(1e-4)
                out_queue.put(item)
                if inorder:
                    ticket.increment()
            except Empty:
                pass

    def publisher():
        for idx, item in enumerate(iterable):
            counter.increment()
            in_queue.put((idx, item))
        stop_event.set()

    Thread(target=publisher).start()
    for _ in range(num_threads):
        Thread(target=worker).start()

    while (not stop_event.is_set()) or counter.current > 0:
        item = out_queue.get()
        yield item
        counter.decrement()

    stop_workers.set()
