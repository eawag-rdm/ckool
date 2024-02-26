import pathlib
import re
import tarfile
from typing import Literal
from zipfile import ZipFile

from tqdm.auto import tqdm

from ckool import TEMPORARY_DIRECTORY_NAME
from ckool.other.types import CompressionTypes
from ckool.other.utilities import partial


def match_via_include_exclude_patters(
    string, include_pattern: str = None, exclude_pattern: str = None
):
    return any(
        [
            include_pattern is None and exclude_pattern is None,
            include_pattern is None
            and exclude_pattern is not None
            and re.search(exclude_pattern, string) is None,
            include_pattern is not None
            and re.search(include_pattern, string) is not None
            and exclude_pattern is None,
            include_pattern is not None
            and re.search(include_pattern, string) is not None
            and exclude_pattern is not None
            and re.search(exclude_pattern, string) is None,
        ]
    )


def iter_files(
    folder: pathlib.Path,
    include_pattern: str = None,
    exclude_pattern: str = None,
    tmp_dir_to_ignore: str | None = TEMPORARY_DIRECTORY_NAME,
):
    """
    Using re to filter paths. If both include_pattern and exclude pattern are provided.
    Both will be used, beware of conflicts.
    include_pattern: str [default: None] -> match everything
    exclude_pattern: str [default: None] -> exclude nothing
    """

    if not folder.exists():
        raise NotADirectoryError(
            f"The directory you specified does not exist. '{folder}'"
        )

    for file_or_folder in folder.glob("**/*"):
        fof = file_or_folder.as_posix()
        if tmp_dir_to_ignore and tmp_dir_to_ignore in fof:
            continue

        if match_via_include_exclude_patters(fof, include_pattern, exclude_pattern):
            if file_or_folder.is_file():
                yield file_or_folder


def generate_archive_destination(
    folder_to_compress: pathlib.Path,
    root_folder: pathlib.Path,
    tmp_dir_name: str = TEMPORARY_DIRECTORY_NAME,
) -> pathlib.Path:
    tmp_folder = root_folder / tmp_dir_name
    tmp_folder.mkdir(exist_ok=True)
    archive_destination = tmp_folder / folder_to_compress.name
    return archive_destination


def zip_files(
    root_folder: pathlib.Path,
    archive_destination: pathlib.Path,
    files: list,
    progressbar: bool = True,
) -> pathlib.Path:
    position = None
    global position_queue
    if "position_queue" in globals():
        position = position_queue.get()

    bar = tqdm(
        files,
        desc=f"Zipping {archive_destination.name}",
        disable=not progressbar,
        position=position,
    )
    with ZipFile(archive_destination.with_suffix(".zip"), mode="w") as _zip:
        for file in files:
            _zip.write(file, file.relative_to(root_folder))
            bar.update()
            bar.refresh()
    bar.close()
    return archive_destination.with_suffix(".zip")


def tar_files(
    root_folder: pathlib.Path,
    archive_destination: pathlib.Path,
    files: list,
    compression: Literal["gz", "bz2", "xz"] = "gz",
    progressbar: bool = True,
) -> pathlib.Path:
    position = None
    global position_queue
    if "position_queue" in globals():
        position = position_queue.get()

    bar = tqdm(
        files,
        desc=f"Taring {archive_destination.name}",
        disable=not progressbar,
        position=position,
    )
    with tarfile.open(
        archive_destination.with_suffix(f".tar.{compression}"), mode=f"w:{compression}"
    ) as tar:
        for file in files:
            tarinfo = tarfile.TarInfo(file.relative_to(root_folder).as_posix())
            tarinfo.size = (
                file.stat().st_size
            )  # size needs to be set, otherwise 0 bytes will be read from each file
            with file.open("rb") as f:
                tar.addfile(tarinfo, f)
            bar.update()
            bar.refresh()
    bar.close()
    return archive_destination.with_suffix(f".tar.{compression}")


def find_archive(archive_destination: pathlib.Path):
    found = []
    for f in iter_files(archive_destination.parent, tmp_dir_to_ignore=""):
        if not f.suffix.endswith(".json") and f.name.startswith(
            archive_destination.name
        ):
            found.append(f)
    assert len(found) <= 1, f"Invalid: Multiple archives found: {repr(found)}"
    if found:
        return found[0]


def get_compression_func(
    compression_type: CompressionTypes = CompressionTypes.zip,
):
    if compression_type == CompressionTypes.zip:
        return zip_files
    elif compression_type in [
        CompressionTypes.tar_gz,
        CompressionTypes.tar_xz,
        CompressionTypes.tar_bz2,
    ]:
        return partial(tar_files, compression=compression_type.value.split(".")[-1])


def iter_package(
    package: pathlib.Path,
    include_pattern: str = None,
    exclude_pattern: str = None,
    tmp_dir_name: str = TEMPORARY_DIRECTORY_NAME,
    ignore_tmp_dir: bool = True,
) -> dict:
    """
    This function gets everything ready for the package upload.
    - it creates a tmp directory and saves compressed folders in there and collects all folders.
    """

    for file_or_folder in package.iterdir():
        if not match_via_include_exclude_patters(
            file_or_folder.as_posix(), include_pattern, exclude_pattern
        ):
            continue

        if file_or_folder.is_file():
            yield {"file": file_or_folder, "folder": {}}
        elif file_or_folder.is_dir():
            if ignore_tmp_dir and TEMPORARY_DIRECTORY_NAME in file_or_folder.as_posix():
                continue
            files_to_compress = list(
                iter_files(file_or_folder, include_pattern, exclude_pattern)
            )
            archive_destination = generate_archive_destination(
                file_or_folder, file_or_folder.parent, tmp_dir_name
            )

            if not files_to_compress:
                continue

            elif file := find_archive(archive_destination):
                yield {"file": file, "folder": {}}
            else:
                yield {
                    "file": "",
                    "folder": {
                        "location": file_or_folder,
                        "files": files_to_compress,
                        "archive_destination": archive_destination,
                        "root_folder": file_or_folder.parent,
                    },
                }

        else:
            raise ValueError(
                f"Ooops this shouldn't happen. This is not a file and not a folder '{file_or_folder.as_posix()}'."
            )


# TODO: this needs rewriting! The returned value must be more consistent. both should return
#  func, args, kwargs An the function is somewhat of a task scheduler
# def iter_package_and_prepare_for_upload(
#     package: pathlib.Path,
#     include_pattern: str = None,
#     exclude_pattern: str = None,
#     compression_func: Callable = zip_files,
#     tmp_dir_name: str = TEMPORARY_DIRECTORY,
# ) -> dict:
#     """
#     This function gets everything ready for the package upload.
#     - it creates a tmp directory and saves compressed folders in there and collects all folders.
#     """
#
#     for file_or_folder in package.iterdir():
#         if not match_via_include_exclude_patters(
#             file_or_folder.as_posix(), include_pattern, exclude_pattern
#         ):
#             continue
#
#         if file_or_folder.is_file():
#             yield {"static": file_or_folder, "dynamic": {}}
#         elif file_or_folder.is_dir():
#             archive_destination = generate_archive_destination(
#                 file_or_folder, file_or_folder.parent, tmp_dir_name
#             )
#             files_to_compress = list(
#                 iter_files(file_or_folder, include_pattern, exclude_pattern)
#             )
#
#             if not files_to_compress:
#                 continue
#             elif archive_destination.with_suffix(".json").exists():
#                 archive_destination.iterdir()
#                 yield {"static": find_archive(archive_destination), "dynamic": {}}
#
#             yield {
#                 "static": "",
#                 "dynamic": {
#                     "func": compression_func,
#                     "args": [],
#                     "kwargs": {
#                         "root_folder": package,
#                         "archive_destination": archive_destination,
#                         "files": files_to_compress,
#                     },
#                 },
#             }
#         else:
#             raise ValueError(
#                 f"Ooops this shouldn't happen. This is not a file and not a folder '{file_or_folder.as_posix()}'."
#             )


def stats_file(file: pathlib.Path, tmp_dir: str = TEMPORARY_DIRECTORY_NAME):
    if file.parent.name == tmp_dir:
        tmp_dir = ""
    return file.parent / tmp_dir / (file.name + ".json")


# class LocalProcessor:
#     def __init__(
#         self,
#         hash_type: HashTypes,
#         cache_file_name: str = "file_meta.json",
#     ):
#         self.hash_type = hash_type
#
#     def get_hash(self, file_path):
#         hash_func = get_hash_func(self.hash_type)
#         return hash_func(file_path)
#
#     def get_size(self, file_path):
#         return file_path.stat().st_size
#
#     def process(self, static_or_dynamic):
#         if file := static_or_dynamic.get("static"):
#             _meta = {
#                 "file": str(file),
#                 "hash": self.get_hash(file),
#                 "hash_type": self.hash_type.value,
#                 "size": self.get_size(file),
#             }
#             update_cache(_meta, stats_file(file, TEMPORARY_DIRECTORY))
#             return _meta
#
#         elif instruction := static_or_dynamic.get("dynamic"):
#             file = instruction["func"](
#                 *instruction["args"], **instruction["kwargs"]
#             )  # compressing
#
#             _meta = {
#                 "file": str(file),
#                 "hash": self.get_hash(file),
#                 "hash_type": self.hash_type.value,
#                 "size": self.get_size(file),
#             }
#             update_cache(_meta, stats_file(file, TEMPORARY_DIRECTORY))
#             return _meta
#
#         else:
#             raise ValueError(
#                 f"Ooops, this is not a valid Processor instruction '{static_or_dynamic}'."
#             )
#
#
# def prepare_for_upload_sequential(
#     package: pathlib.Path,
#     include_pattern: str = None,
#     exclude_pattern: str = None,
#     compression_type: CompressionTypes = CompressionTypes.zip,
#     tmp_dir_name: str = TEMPORARY_DIRECTORY,
#     hash_type: HashTypes = HashTypes.sha256,
# ):
#     compression_func = get_compression_func(compression_type)
#     files_to_upload = []
#     for static_or_dynamic in iter_package_and_prepare_for_upload(
#         package, include_pattern, exclude_pattern, compression_func, tmp_dir_name
#     ):
#         lp = LocalProcessor(hash_type)
#         file_info = lp.process(static_or_dynamic)
#         files_to_upload.append(file_info)
#
#     return files_to_upload
#
#
# def prepare_for_upload_parallel(
#     package: pathlib.Path,
#     include_pattern: str = None,
#     exclude_pattern: str = None,
#     compression_type: CompressionTypes = CompressionTypes.zip,
#     tmp_dir_name: str = TEMPORARY_DIRECTORY,
#     hash_type: HashTypes = HashTypes.sha256,
#     max_workers: int = None,
# ):
#     compression_func = get_compression_func(compression_type)
#     max_workers = max_workers if max_workers is not None else os.cpu_count()
#     if not isinstance(max_workers, int):
#         raise ValueError(
#             f"The value for 'max_worker' must be an integer. "
#             f"Your device allow up to '{os.cpu_count()}' parallel workers."
#         )
#
#     lp = LocalProcessor(hash_type)
#
#     files_to_upload = []
#     with ProcessPoolExecutor(max_workers=max_workers) as executor:
#         for result in executor.map(
#             lp.process,
#             iter_package_and_prepare_for_upload(
#                 package,
#                 include_pattern,
#                 exclude_pattern,
#                 compression_func,
#                 tmp_dir_name,
#             ),
#         ):
#             files_to_upload.append(result)
#
#     return files_to_upload
