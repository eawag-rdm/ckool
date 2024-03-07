import multiprocessing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Callable


def map_function_with_threadpool(
    func: Callable, args: list | None = None, kwargs: list[dict] | None = None
) -> list:
    """
    Maps a function over arguments and keyword arguments using a ThreadPoolExecutor.

    :param func: The function to be executed.
    :param args: An iterable of tuples, each representing the positional arguments to `func`.
    :param kwargs: An iterable of dictionaries, each representing the keyword arguments to `func`.
    :return: A list containing the result of each function call.
    """
    if kwargs is None:
        kwargs = [{}] * len(args)
    if args is None:
        args = [[]] * len(kwargs)

    results = []
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(func, *args, **kwargs) for args, kwargs in zip(args, kwargs)
        ]
        for future in futures:
            results.append(future.result())
    return results


def worker_init(q):
    """Initialize global queue for worker processes."""
    global position_queue
    position_queue = q


def map_function_with_processpool(
    func: Callable,
    args: list | None = None,
    kwargs: list[dict] | None = None,
    workers: int | None = 4,
) -> list:
    """
    Maps a function over arguments and keyword arguments using a ThreadPoolExecutor.

    :param func: The function to be executed.
    :param args: An iterable of tuples, each representing the positional arguments to `func`.
    :param kwargs: An iterable of dictionaries, each representing the keyword arguments to `func`.
    :param workers: How many workers to use.
    :return: A list containing the result of each function call.
    """
    if kwargs is None:
        kwargs = [{}] * len(args)
    if args is None:
        args = [[]] * len(kwargs)

    with multiprocessing.Manager() as manager:
        position_queue = manager.Queue()
        [position_queue.put(i) for i in range(10_000)]
        results = []
        with ProcessPoolExecutor(
            max_workers=workers, initializer=worker_init, initargs=(position_queue,)
        ) as executor:
            futures = [
                executor.submit(func, *args, **kwargs)
                for args, kwargs in zip(args, kwargs)
            ]
            for future in futures:
                if res := future.result():
                    results.append(res)

    return results
