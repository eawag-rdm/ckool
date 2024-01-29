import os
import pathlib
import re
import tarfile
from concurrent.futures import ProcessPoolExecutor
from typing import Literal
from zipfile import ZipFile

from .hashing import get_hash_func


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
    folder: pathlib.Path, include_pattern: str = None, exclude_pattern: str = None
):
    """
    Using re to filter paths. If both include_pattern and exclude pattern are provided.
    Both will be used, beware of conflicts.
    include_pattern: str [default: None] -> match everything
    exclude_pattern: str [default: None] -> exclude nothing
    """

    for file_or_folder in folder.glob("**/*"):
        fof = file_or_folder.as_posix()
        if match_via_include_exclude_patters(fof, include_pattern, exclude_pattern):
            if file_or_folder.is_file():
                yield file_or_folder


def generate_archive_dest(
    folder_to_zip: pathlib.Path,
    root_folder: pathlib.Path,
    tmp_dir_name: str = ".ckool",
) -> pathlib.Path:
    tmp_folder = root_folder / tmp_dir_name
    tmp_folder.mkdir(exist_ok=True)
    archive_destination = tmp_folder / folder_to_zip.name
    return archive_destination


def zip_files(
    root_folder: pathlib.Path, archive_destination: pathlib.Path, files: list
) -> pathlib.Path:
    with ZipFile(archive_destination.with_suffix(".zip"), mode="w") as zip:
        for file in files:
            zip.write(file, file.relative_to(root_folder))

    return archive_destination.with_suffix(".zip")


def tar_files(
    root_folder: pathlib.Path,
    archive_destination: pathlib.Path,
    files: list,
    compression: Literal["gz", "bz2", "xz"] = "gz",
) -> pathlib.Path:
    with tarfile.open(
        archive_destination.with_suffix(f".tar.{compression}"), mode=f"w:{compression}"
    ) as tar:
        for file in files:
            tarinfo = tarfile.TarInfo(file.relative_to(root_folder).as_posix())
            tarinfo.size = (
                file.stat().st_size
            )  # size needs to be set, otherwise 0 bytes will be read ffrom each file
            with file.open("rb") as f:
                tar.addfile(tarinfo, f)
    return archive_destination.with_suffix(f".tar.{compression}")


def iter_package_and_prepare_for_upload(
    package: pathlib.Path,
    include_pattern: str = None,
    exclude_pattern: str = None,
    compression_type: Literal["zip", "tar"] = "zip",
    tmp_dir_name: str = ".ckool",
) -> dict:
    """
    This function gets everything ready for the package upload.
    - it creates a tmp directory and saves compressed folders in there and collects all folders.
    """
    compress = {"zip": zip_files, "tar": tar_files}.get(compression_type)

    if not package.exists():
        raise NotADirectoryError(
            f"The directory you specified does not exist. '{package}'"
        )

    for file_or_folder in package.iterdir():
        if not match_via_include_exclude_patters(
            file_or_folder.as_posix(), include_pattern, exclude_pattern
        ):
            continue

        if file_or_folder.is_file():
            yield {"static": file_or_folder, "dynamic": {}}
        elif file_or_folder.is_dir():
            archive_destination = generate_archive_dest(
                file_or_folder, file_or_folder.parent, tmp_dir_name
            )
            files_to_compress = list(
                iter_files(file_or_folder, include_pattern, exclude_pattern)
            )

            if not files_to_compress:
                continue

            yield {
                "static": "",
                "dynamic": {
                    "func": compress,
                    "args": [],
                    "kwargs": {
                        "root_folder": package,
                        "archive_destination": archive_destination,
                        "files": files_to_compress,
                    },
                },
            }
        else:
            raise ValueError(
                f"Ooops this shouldn't happen. This is not a file and not a folder '{file_or_folder.as_posix()}'."
            )


class LocalProcessor:
    def __init__(
        self,
        hash_type: str,
    ):
        self.hash_type = hash_type

    def get_hash(self, file_path):
        hash_func = get_hash_func(self.hash_type)
        return hash_func(file_path)

    def get_size(self, file_path):
        return file_path.stat().st_size

    def process(self, static_or_dynamic):
        if file := static_or_dynamic.get("static"):
            return {
                "file": file,
                "hash": self.get_hash(file),
                "size": self.get_size(file),
            }
        elif instruction := static_or_dynamic.get("dynamic"):
            file = instruction["func"](
                *instruction["args"], **instruction["kwargs"]
            )  # compressing
            return {
                "file": file,
                "hash": self.get_hash(file),
                "size": self.get_size(file),
            }
        else:
            raise ValueError(
                f"Ooops, this is not a valid Processor instruction '{static_or_dynamic}'."
            )


def prepare_for_upload_sequential(
    package: pathlib.Path,
    include_pattern: str = None,
    exclude_pattern: str = None,
    compression_type: Literal["zip", "tar"] = "zip",
    tmp_dir_name: str = ".ckool",
    hash_type: str = "sha256",
):
    files_to_upload = []
    for static_or_dynamic in iter_package_and_prepare_for_upload(
        package, include_pattern, exclude_pattern, compression_type, tmp_dir_name
    ):
        lp = LocalProcessor(hash_type)
        file_info = lp.process(static_or_dynamic)
        files_to_upload.append(file_info)

    return files_to_upload


def prepare_for_upload_parallel(
    package: pathlib.Path,
    include_pattern: str = None,
    exclude_pattern: str = None,
    compression_type: Literal["zip", "tar"] = "zip",
    tmp_dir_name: str = ".ckool",
    hash_type: str = "sha256",
    max_workers: int = None,
):
    max_workers = max_workers if max_workers is not None else os.cpu_count()
    if not isinstance(max_workers, int):
        raise ValueError(
            f"The value for 'max_worker' must be an integer. "
            f"Your device allow up to '{os.cpu_count()}' parallel workers."
        )

    lp = LocalProcessor(hash_type)

    files_to_upload = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(
            lp.process,
            iter_package_and_prepare_for_upload(
                package,
                include_pattern,
                exclude_pattern,
                compression_type,
                tmp_dir_name,
            ),
        ):
            files_to_upload.append(result)

    return files_to_upload
