import time

import pytest

from ckool import (
    COMPRESSION_TYPE,
    HASH_TYPE,
    OVERWRITE_FILE_STATS,
    TEMPORARY_DIRECTORY_NAME,
)
from ckool.other.caching import update_cache
from ckool.other.file_management import get_compression_func
from ckool.other.hashing import get_hash_func
from ckool.parallel_runner import ParallelRunner, ParallelType
from ckool.templates import upload_resource_file_via_api

compression_func = get_compression_func(COMPRESSION_TYPE)
hash_func = get_hash_func(HASH_TYPE)


def collect_stats(
    tmp_dir_name, overwrite, hash_func_, hash_type, filepath, progressbar
):
    if tmp_dir_name in filepath.as_posix():
        cache_file = filepath.with_suffix(filepath.suffix + ".json")
    else:
        cache_file = filepath.parent / tmp_dir_name / (filepath.name + ".json")

    if cache_file.exists() and not overwrite:
        pass
    else:
        stats = {
            "file": str(filepath),
            "hash": hash_func_(filepath=filepath, progressbar=progressbar),
            "hash_type": hash_type,
            "size": filepath.stat().st_size,
        }

        update_cache(stats, cache_file)
        return [filepath, cache_file]


def build_start_conditions_for_parallel_runner(
    package_dir, tmp_dir_name, overwrite, hash_type, compression_func_
):
    start_conditions = []
    for static_or_dynamic in package_dir(
        package_dir,
        None,
        None,
        TEMPORARY_DIRECTORY_NAME,
    ):
        if dynamic := static_or_dynamic.get("folder"):
            dynamic["kwargs"].update({"progressbar": False})
            start_conditions.append(dynamic)
        elif filepath := static_or_dynamic.get("file"):
            start_conditions.append(
                {
                    "func": collect_stats,
                    "args": [],
                    "kwargs": dict(
                        filepath=filepath,
                        overwrite=overwrite,
                        tmp_dir_name=tmp_dir_name,
                        hash_func_=hash_func,
                        hash_type=hash_type,
                        hash_func_args=[],
                        hash_func_kwargs=dict(filepath=filepath),
                    ),
                }
            )
    return start_conditions


def run_in_process_1(x, y):
    time.sleep(0.1)
    return x + y


def run_in_process_2(x, y):
    time.sleep(0.2)
    return x * y


def run_in_thread_1(x, y=0):
    time.sleep(0.05)
    return 2 * (x + y)


def run_in_process_with_error(*args, **kwargs):
    raise ValueError("Oops")


def run_in_thread_with_error(*args, **kwargs):
    raise ValueError("Oops")


@pytest.mark.impure
def test_parallel_runner():
    trigger_map = {
        run_in_process_1.__name__: {
            "next_func": run_in_process_2.__name__,
            "pass_return_values": True,
        },
        run_in_process_2.__name__: {
            "next_func": run_in_thread_1.__name__,
            "pass_return_values": False,
        },
    }
    start_conditions = [
        {"func": run_in_process_1, "args": [3, 4], "kwargs": {}},
        {"func": run_in_process_1, "args": [3], "kwargs": {"y": 9}},
        {"func": run_in_process_2, "args": [2, 2], "kwargs": {}},
        {"func": run_in_thread_1, "args": [3, 4], "kwargs": {}},
    ]
    func_type = {
        run_in_process_1.__name__: ParallelType.process,
        run_in_process_2.__name__: ParallelType.process,
        run_in_thread_1.__name__: ParallelType.thread,
    }
    base_func_info = {
        # "func_1": {"func": func_1, "args": [], "kwargs": {}},
        run_in_process_2.__name__: {
            "func": run_in_process_2,
            "args": [],
            "kwargs": {"y": 3},
        },
        run_in_thread_1.__name__: {
            "func": run_in_thread_1,
            "args": [1],
            "kwargs": {"y": 2},
        },
    }

    pr = ParallelRunner(trigger_map, start_conditions, func_type, base_func_info)
    assert pr.queue_multiprocessing.qsize() == 3
    assert pr.queue_multithreading.qsize() == 1
    pr.run_flow()
    assert pr.queue_finished_threads.qsize() == 4
    assert pr.queue_finished_processes.qsize() == 5


@pytest.mark.impure
def test_parallel_runner_with_errors():
    trigger_map, base_func_info = {}, {}
    start_conditions = [
        {"func": run_in_process_with_error, "args": [3, 4], "kwargs": {}},
        {"func": run_in_thread_with_error, "args": [3], "kwargs": {"y": 9}},
    ]
    func_type = {
        run_in_process_with_error.__name__: ParallelType.process,
        run_in_thread_with_error.__name__: ParallelType.thread,
    }

    pr = ParallelRunner(trigger_map, start_conditions, func_type, base_func_info)
    assert pr.queue_multiprocessing.qsize() == 1
    assert pr.queue_multithreading.qsize() == 1
    with pytest.raises(ExceptionGroup):
        pr.run_flow()

    del pr


@pytest.mark.skip
@pytest.mark.impure
@pytest.mark.slow
def test_parallel_runner_realistic(
    very_large_package, ckan_instance, ckan_envvars, ckan_setup_data
):
    trigger_map = {
        compression_func.__name__: {
            "next_func": collect_stats.__name__,
            "pass_return_values": True,
        },
        collect_stats.__name__: {
            "next_func": upload_resource_file_via_api.__name__,
            "pass_return_values": True,
        },
    }
    start_conditions = build_start_conditions_for_parallel_runner(
        very_large_package,
        TEMPORARY_DIRECTORY_NAME,
        OVERWRITE_FILE_STATS,
        HASH_TYPE,
        compression_func_=compression_func,
    )
    func_type = {
        compression_func.__name__: ParallelType.process,
        collect_stats.__name__: ParallelType.process,
        upload_resource_file_via_api.__name__: ParallelType.thread,
    }
    base_func_info = {
        compression_func.__name__: {
            "func": compression_func,
            "args": [],
            "kwargs": {"progressbar": False},
        },
        collect_stats.__name__: {
            "func": collect_stats,
            "args": [
                TEMPORARY_DIRECTORY_NAME,
                OVERWRITE_FILE_STATS,
                hash_func,
                HASH_TYPE,
            ],
            "kwargs": {"progressbar": False},
        },
        upload_resource_file_via_api.__name__: {
            "func": upload_resource_file_via_api,
            "args": [
                {
                    "server": ckan_instance.server,
                    "token": ckan_instance.token,
                    "verify_certificate": ckan_instance.verify,
                },
                ckan_envvars["test_package"],
            ],
            "kwargs": {"progressbar": False},
        },
    }

    pr = ParallelRunner(trigger_map, start_conditions, func_type, base_func_info)
    pr.run_flow()
