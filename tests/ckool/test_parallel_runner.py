import time

import pytest

from ckool import COMPRESSION_TYPE, HASH_TYPE, OVERWRITE_FILE_STATS, TEMPORARY_DIRECTORY
from ckool.ckan.ckan import CKAN
from ckool.other.caching import read_cache, update_cache
from ckool.other.file_management import (
    get_compression_func,
    iter_package_and_prepare_for_upload,
)
from ckool.other.hashing import get_hash_func
from ckool.parallel_runner import ParallelRunner, ParallelType


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


compression_func = get_compression_func(COMPRESSION_TYPE)


def collect_stats(tmp_dir_name, overwrite, hash_type, filepath):
    if tmp_dir_name in filepath.as_posix():
        cache_file = filepath.with_suffix(filepath.suffix + ".json")
    else:
        cache_file = filepath.parent / tmp_dir_name / (filepath.name + ".json")

    if cache_file.exists() and not overwrite:
        pass
    else:
        stats = {
            "file": str(filepath),
            "hash": get_hash_func(hash_type)(filepath=filepath),
            "hash_type": hash_type,
            "size": filepath.stat().st_size,
        }

        update_cache(stats, cache_file)
        return [filepath, cache_file]


def upload(ckan_api_input, package_name, filepath, cache_file):
    ckan_instance = CKAN(**ckan_api_input)
    stats = read_cache(cache_file)
    ckan_instance.create_resource_of_type_file(
        file=filepath,
        package_id=package_name,
        file_hash=stats["hash"],
        file_size=stats["size"],
        hash_type=stats["hash_type"],
    )


def build_start_conditions(package_dir, tmp_dir_name, overwrite, hash_type):
    start_conditions = []
    for static_or_dynamic in iter_package_and_prepare_for_upload(
        package_dir,
        None,
        None,
        compression_func,
        TEMPORARY_DIRECTORY,
    ):
        if dynamic := static_or_dynamic.get("dynamic"):
            start_conditions.append(dynamic)
        elif filepath := static_or_dynamic.get("static"):
            start_conditions.append(
                {
                    "func": collect_stats,
                    "args": [],
                    "kwargs": dict(
                        filepath=filepath,
                        overwrite=overwrite,
                        tmp_dir_name=tmp_dir_name,
                        hash_type=hash_type,
                        hash_func_args=[],
                        hash_func_kwargs=dict(filepath=filepath),
                    ),
                }
            )
    return start_conditions


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
            "next_func": upload.__name__,
            "pass_return_values": True,
        },
    }
    start_conditions = build_start_conditions(
        very_large_package, TEMPORARY_DIRECTORY, OVERWRITE_FILE_STATS, HASH_TYPE
    )
    func_type = {
        compression_func.__name__: ParallelType.process,
        collect_stats.__name__: ParallelType.process,
        upload.__name__: ParallelType.thread,
    }
    base_func_info = {
        compression_func.__name__: {
            "func": compression_func,
            "args": [],
            "kwargs": {},
        },
        collect_stats.__name__: {
            "func": collect_stats,
            "args": [TEMPORARY_DIRECTORY, OVERWRITE_FILE_STATS, HASH_TYPE],
            "kwargs": {},
        },
        upload.__name__: {
            "func": upload,
            "args": [
                {
                    "server": ckan_instance.server,
                    "token": ckan_instance.token,
                    "verify_certificate": ckan_instance.verify,
                },
                ckan_envvars["test_package"],
            ],
            "kwargs": {},
        },
    }

    pr = ParallelRunner(trigger_map, start_conditions, func_type, base_func_info)

    pr.run_flow()
