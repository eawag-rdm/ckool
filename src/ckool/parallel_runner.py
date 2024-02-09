import multiprocessing
import queue
import threading
import time
import typing
from enum import Enum


class ParallelType(str, Enum):
    thread = "thread"
    process = "process"


def traverse_trigger_map(function, trigger_map):
    spawned_calls = [function]
    while next_function := trigger_map.get(function):
        spawned_calls.append(next_function_name := next_function["next_func"])
        function = next_function_name
    return spawned_calls


def build_trigger_type_map(trigger_map, function_types):
    return {
        func: [function_types[func], function_types[next_func]]
        for func, (next_func, _) in trigger_map.items()
    }


def collect_all_called_func_types(start_conditions, trigger_map, function_types):
    func_types = []
    for entry in start_conditions:
        func_name = entry["func"].__name__
        func_calls = traverse_trigger_map(func_name, trigger_map)
        func_types += [function_types[func] for func in func_calls]
    return func_types


def estimate_total_processes_and_threads_that_will_run(
    total_job_estimate, start_conditions, trigger_map, func_type
):
    all_func_types = collect_all_called_func_types(
        start_conditions, trigger_map, func_type
    )

    for typ in all_func_types:
        if typ == ParallelType.thread:
            total_job_estimate["multi-threaded"] += 1
        elif typ == ParallelType.process:
            total_job_estimate["multi-processed"] += 1
        else:
            raise ValueError(
                f"The type '{typ}' for parallel computation is not defined."
            )


def extract_job_wrapper_input(
    function_name, parallel_type_to_queue_map, func_type, trigger_map, base_func_info
):
    _next = trigger_map.get(function_name, False)

    if _next:
        next_func_name, pass_return = _next["next_func"], _next["pass_return_values"]
        queue_info = parallel_type_to_queue_map[
            repr([func_type[function_name], func_type[next_func_name]])
        ]
        return {
            "queue_finished": queue_info["done"],
            "queue_todo": queue_info["next"],
            "queue_error": queue_info["errors"],
            "pass_return": pass_return,
            "trigger_info": base_func_info[next_func_name],
        }
    else:
        queue_info = parallel_type_to_queue_map[
            repr([func_type[function_name], func_type[function_name]])
        ]
        return {
            "queue_finished": queue_info["done"],
            "queue_error": queue_info["errors"],
            "queue_todo": None,
            "pass_return": False,
            "trigger_info": {},
        }


class JobWrapper:
    def __init__(
        self, job_id, func, args, kwargs, queue_todo, queue_finished, queue_error
    ):
        self.job_id = job_id
        self.func = func
        self.args = args
        self.kwargs = kwargs

        self.queue_todo = queue_todo
        self.queue_finished = queue_finished
        self.queue_error = queue_error

        self.with_trigger = False
        self.include_return_values = False

        self.next_func = None
        self.next_args = None
        self.next_kwargs = None

    def add_trigger(self, next_func, next_args, next_kwargs):
        self.with_trigger = True
        self.next_func = next_func
        self.next_args = next_args
        self.next_kwargs = next_kwargs

    def set_include_return_values(self):
        self.include_return_values = True

    def run(self):
        try:
            returns = self.func(*self.args, **self.kwargs)
            next_instructions = {
                "func": self.next_func,
                "args": self.next_args,
                "kwargs": self.next_kwargs,
            }
            self.queue_finished.put(
                {"done": self.func.__name__, "returned": returns, "job_id": self.job_id}
            )
            if self.include_return_values:
                if isinstance(returns, list):
                    next_instructions["args"] += returns
                elif isinstance(returns, dict):
                    next_instructions["kwargs"].update(returns)
                elif not isinstance(returns, typing.Iterable):
                    next_instructions["args"] += [returns]
                else:
                    raise ValueError(
                        f"Functions run with ParallelRunner, "
                        f"that pass return values to other functions must return non iterables, lists or dicts. "
                        f"Your returned '{type(returns)}'."
                    )
            if self.with_trigger:
                self.queue_todo.put(next_instructions)
        except Exception as exc:
            exc.add_note(f"Function '{self.func.__name__}' caused the error.")
            self.queue_error.put(exc)


def wrap_job(
    _id, job, queue_finished, queue_todo, queue_error, pass_return, trigger_info
):
    _job = JobWrapper(
        job_id=_id,
        func=job["func"],
        args=job["args"],
        kwargs=job["kwargs"],
        queue_todo=queue_todo,
        queue_finished=queue_finished,
        queue_error=queue_error,
    )
    if pass_return:
        _job.set_include_return_values()
    if trigger_info:
        _job.add_trigger(
            trigger_info["func"], trigger_info["args"], trigger_info["kwargs"]
        )
    return _job


class ParallelRunner:
    """
    This task is for dynamically scheduling chains of function executions
    that can either be multi-processed or multi-threaded
    """

    def __init__(
        self,
        trigger_map: dict,
        start_conditions: list[dict],
        func_type: dict,
        base_func_info: dict,
        wait_between_checks: float = 0.1,
    ):
        self.trigger_map = trigger_map
        self.start_conditions = start_conditions
        self.func_type = func_type
        self.base_func_info = base_func_info
        self.wait_between_checks = wait_between_checks

        self.wait_between_checks: float = 0.1
        self.total_job_estimate = {"multi-threaded": 0, "multi-processed": 0}

        self.job_id = 0

        self.queue_multiprocessing = multiprocessing.Queue()
        self.queue_multiprocessing_cache = multiprocessing.Queue()
        self.queue_multithreading = queue.Queue()
        self.queue_multithreading_cache = queue.Queue()

        self.queue_finished_processes = multiprocessing.Queue()
        self.queue_finished_threads = queue.Queue()

        self.queue_process_errors = multiprocessing.Queue()
        self.queue_thread_errors = queue.Queue()

        self.parallel_type_to_queue_map = {
            repr([ParallelType.process, ParallelType.process]): {
                "done": self.queue_finished_processes,
                "next": self.queue_multiprocessing,
                "errors": self.queue_process_errors,
            },
            repr([ParallelType.process, ParallelType.thread]): {
                "done": self.queue_finished_processes,
                "next": self.queue_multiprocessing_cache,
                "errors": self.queue_process_errors,
            },
            repr([ParallelType.thread, ParallelType.process]): {
                "done": self.queue_finished_threads,
                "next": self.queue_multithreading_cache,
                "errors": self.queue_thread_errors,
            },
            repr([ParallelType.thread, ParallelType.thread]): {
                "done": self.queue_finished_threads,
                "next": self.queue_multithreading,
                "errors": self.queue_thread_errors,
            },
        }

        self.threads = []
        self.processes = []

        estimate_total_processes_and_threads_that_will_run(
            self.total_job_estimate,
            self.start_conditions,
            self.trigger_map,
            self.func_type,
        )
        self._populate_queues()

    def _get_total_id(self):
        total = self.job_id
        self.job_id += 1
        return total

    def _run_thread_worker(self, multithreading_queue):
        if multithreading_queue.empty():
            return
        job = multithreading_queue.get()
        _job = wrap_job(
            self._get_total_id(),
            job,
            **extract_job_wrapper_input(
                job["func"].__name__,
                self.parallel_type_to_queue_map,
                self.func_type,
                self.trigger_map,
                self.base_func_info,
            ),
        )
        thread = threading.Thread(target=_job.run)
        self.threads.append(thread)
        thread.start()

    def _run_process_worker(self, multiprocessing_queue):
        if multiprocessing_queue.empty():
            return
        job = multiprocessing_queue.get()
        _job = wrap_job(
            self._get_total_id(),
            job,
            **extract_job_wrapper_input(
                job["func"].__name__,
                self.parallel_type_to_queue_map,
                self.func_type,
                self.trigger_map,
                self.base_func_info,
            ),
        )
        process = multiprocessing.Process(target=_job.run)
        self.processes.append(process)
        process.start()

    def _populate_queues(self):
        for job in self.start_conditions:
            typ = self.func_type[job["func"].__name__]
            if typ == ParallelType.thread:
                self.queue_multithreading.put(job)
            elif typ == ParallelType.process:
                self.queue_multiprocessing.put(job)

    def _raise_if_errors(self):
        exceptions = []
        while not self.queue_process_errors.empty():
            exceptions.append(self.queue_process_errors.get())

        while not self.queue_thread_errors.empty():
            exceptions.append(self.queue_thread_errors.get())

        if exceptions:
            self._join_threads_and_processes()
            raise ExceptionGroup("Exceptions occurred in ParallelRunner:", exceptions)

    def _empty_job_cache(self):
        while not self.queue_multiprocessing_cache.empty():
            self.queue_multithreading.put(self.queue_multiprocessing_cache.get())

        while not self.queue_multithreading_cache.empty():
            self.queue_multiprocessing.put(self.queue_multithreading_cache.get())

    def _join_threads_and_processes(self):
        for process in self.processes:
            process.join()

        for thread in self.threads:
            thread.join()

    def run_flow(self):
        while (
            self.queue_finished_threads.qsize() + self.queue_finished_processes.qsize()
            < self.total_job_estimate["multi-threaded"]
            + self.total_job_estimate["multi-processed"]
        ):
            self._raise_if_errors()

            self._empty_job_cache()

            time.sleep(self.wait_between_checks)

            self._run_thread_worker(self.queue_multithreading)
            self._run_process_worker(self.queue_multiprocessing)

        self._join_threads_and_processes()
