# import dataclasses
# import multiprocessing
# import pathlib
# import queue
# import threading
# import time
# from enum import Enum
#
# import tqdm
#
# from ckool import TEMPORARY_DIRECTORY
# from ckool.ckan.ckan import CKAN
# from ckool.other.caching import read_cache, update_cache
# from ckool.other.file_management import (
#     iter_package_and_prepare_for_upload,
# )
# from ckool.other.types import CompressionTypes
# from ckool.other.hashing import get_hash_func
#
#
# class ProgressBarTypes(str, Enum):
#     compress = "compress"
#     hash = "hash"
#     upload = "upload"
#
#
# @dataclasses.dataclass
# class FullUploadFlow:
#     package_name: str
#     package_folder: pathlib.Path
#     include_pattern: str = None
#     exclude_pattern: str = None
#     compression_type: CompressionTypes = "zip"
#     hash_type: str = "sha256"
#     tmp_dir_name: str = TEMPORARY_DIRECTORY
#     ckan_api_input: dict = None
#     overwrite: bool = False
#
#     total_job_estimate: dict = None
#     wait_between_checks: float = 0.1
#     progressbar_position = 0
#
#     queue_multiprocessing = multiprocessing.Queue()
#     queue_multiprocessing_cache = multiprocessing.Queue()
#     queue_multithreading = queue.Queue()
#
#     queue_finished_processes = multiprocessing.Queue()
#     queue_finished_threads = queue.Queue()
#
#     threads = []
#     processes = []
#
#     __hash_block_size: int = 65536
#     __map_method_to_pbar_typ = {
#         "_compress": ProgressBarTypes.compress,
#         "_collect_stats": ProgressBarTypes.hash,
#         "_upload": ProgressBarTypes.upload,
#     }
#
#     def __post_init__(self):
#         self._setup_tasks_to_be_processed_via_iterating_the_package()
#
#     def _estimate_total_processes_and_threads_that_will_run(self, static, dynamic):
#         self.total_job_estimate = {
#             "multi-threaded": static + dynamic,
#             "multi-processed": static + 2 * dynamic,
#         }
#
#     def _setup_progressbar(
#         self,
#         method_name: str,
#         filepath: pathlib.Path = None,
#         position: int = None,
#         instruction: dict = None,
#         *args,
#         **kwargs,
#     ):
#         typ = self.__map_method_to_pbar_typ[method_name]
#         if typ == ProgressBarTypes.compress:
#             files = instruction["kwargs"]["files"]
#             archive_destination = instruction["kwargs"]["archive_destination"]
#             return tqdm.tqdm(
#                 files, desc=f"Compressing {archive_destination.name}", position=position
#             )
#         elif typ == ProgressBarTypes.hash:
#             iterations = filepath.stat().st_size / self.__hash_block_size
#             return tqdm.tqdm(
#                 total=int(iterations) + 1,
#                 desc=f"Hashing {filepath.name}",
#                 position=position,
#             )
#         elif typ == ProgressBarTypes.upload:
#             return tqdm.tqdm(
#                 total=filepath.stat().st_size,
#                 unit="B",
#                 unit_scale=True,
#                 desc=f"Uploading {filepath.name}",
#             )
#         else:
#             raise ValueError(f"Unknown progressbar type '{typ}'.")
#
#     def _setup_tasks_to_be_processed_via_iterating_the_package(self):
#         static = 0
#         dynamic = 0
#         for static_or_dynamic in iter_package_and_prepare_for_upload(
#             self.package_folder,
#             self.include_pattern,
#             self.exclude_pattern,
#             self.compression_type,
#             self.tmp_dir_name,
#         ):
#             if file := static_or_dynamic.get("static"):
#                 static += 1
#                 self.queue_multiprocessing.put(
#                     {
#                         "func": self._collect_stats,
#                         "args": [],
#                         "kwargs": {"filepath": file},
#                     }
#                 )
#             elif instruction := static_or_dynamic.get("dynamic"):
#                 dynamic += 1
#                 self.queue_multiprocessing.put(
#                     {
#                         "func": self._compress,
#                         "args": [],
#                         "kwargs": {"instruction": instruction},
#                     }
#                 )
#         self._estimate_total_processes_and_threads_that_will_run(static, dynamic)
#
#     def _compress(self, instruction: dict, pbar: tqdm.tqdm = None):
#         instruction.update({"progressbar": pbar})
#         file = instruction["func"](*instruction["args"], **instruction["kwargs"])
#         self.queue_finished_processes.put({"done": self._compress.__name__})
#         self.queue_multiprocessing.put(
#             {"func": self._collect_stats, "args": [], "kwargs": {"filepath": file}}
#         )
#
#     def _collect_stats(self, filepath: pathlib.Path, pbar: tqdm.tqdm = None):
#         if self.tmp_dir_name in filepath.as_posix():
#             cache_file = filepath.with_suffix(filepath.suffix + ".json")
#         else:
#             cache_file = (
#                 filepath.parent / TEMPORARY_DIRECTORY / (filepath.name + ".json")
#             )
#
#         if cache_file.exists() and not self.overwrite:
#             pass
#         else:
#             stats = {
#                 "file": str(filepath),
#                 "hash": get_hash_func(self.hash_type)(
#                     filepath=filepath,
#                     block_size=self.__hash_block_size,
#                     progressbar=pbar,
#                 ),
#                 "hash_type": self.hash_type,
#                 "size": filepath.stat().st_size,
#             }
#
#             update_cache(stats, cache_file)
#
#         self.queue_finished_processes.put({"done": self._collect_stats.__name__})
#         self.queue_multiprocessing_cache.put(
#             {
#                 "func": self._upload,
#                 "args": [],
#                 "kwargs": {"filepath": filepath, "cache_file": cache_file},
#             }
#         )
#
#     def _upload(
#         self, filepath: pathlib.Path, cache_file: pathlib.Path, pbar: tqdm.tqdm = None
#     ):
#         ckan_instance = CKAN(**self.ckan_api_input)
#         stats = read_cache(cache_file)
#         ckan_instance.create_resource_of_type_file(
#             file=filepath,
#             package_id=self.package_name,
#             file_hash=stats["hash"],
#             file_size=stats["size"],
#             hash_type=stats["hash_type"],
#             progressbar=pbar,
#         )
#         self.queue_finished_threads.put({"done": self._upload.__name__})
#
#     def _run_thread_worker(self, multithreading_queue):
#         if multithreading_queue.empty():
#             return
#         job = multithreading_queue.get()
#         thread = threading.Thread(
#             target=job["func"], args=job["args"], kwargs=job["kwargs"]
#         )
#         self.threads.append(thread)
#         thread.start()
#
#     def _run_process_worker(self, multiprocessing_queue):
#         if multiprocessing_queue.empty():
#             return
#         job = multiprocessing_queue.get()
#         process = multiprocessing.Process(
#             target=job["func"], args=job["args"], kwargs=job["kwargs"]
#         )
#         self.processes.append(process)
#         process.start()
#
#     def run_flow(self):
#         while (
#             self.queue_finished_threads.qsize() + self.queue_finished_processes.qsize()
#             < self.total_job_estimate["multi-threaded"]
#             + self.total_job_estimate["multi-processed"]
#         ):
#             while not self.queue_multiprocessing_cache.empty():
#                 self.queue_multithreading.put(self.queue_multiprocessing_cache.get())
#
#             time.sleep(self.wait_between_checks)
#             self._run_thread_worker(self.queue_multithreading)
#             self._run_process_worker(self.queue_multiprocessing)
#
#         for process in self.processes:
#             process.join()
#
#         for thread in self.threads:
#             thread.join()
