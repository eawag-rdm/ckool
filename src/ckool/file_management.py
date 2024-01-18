import pathlib
from tarfile import TarFile, TarInfo
from typing import Literal
from zipfile import ZipFile


def glob_files(folder: pathlib.Path, pattern: str = "**/*"):
    """Matching like fnmatch https://docs.python.org/3/library/fnmatch.html#module-fnmatch"""
    for file_or_folder in folder.glob(pattern):
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
    root_folder: pathlib.Path, archive_destination: pathlib.Path, files: list
) -> pathlib.Path:
    with TarFile(archive_destination.with_suffix(".tar"), mode="w") as tar:
        for file in files:
            tar.addfile(TarInfo(file.relative_to(root_folder).as_posix()), file)
    return archive_destination.with_suffix(".tar")


def prepare_for_package_upload(
    candidate: pathlib.Path,
    pattern: str = "**/*",
    compression_type: Literal["zip", "tar"] = "zip",
    tmp_dir_name: str = ".ckool",
) -> dict:
    compress = {"zip": zip_files, "tar": tar_files}.get(compression_type)

    if candidate.is_file():
        return {"files": [candidate], "created": []}

    elif candidate.is_dir():
        archive_destination = generate_archive_dest(
            candidate, candidate.parent, tmp_dir_name
        )

        files_to_compress = list(glob_files(candidate, pattern))

        if not files_to_compress:
            return {"files": [], "created": []}

        archive = compress(
            candidate,
            archive_destination,
            files_to_compress,
        )

        return {"files": [archive], "created": [archive]}

    else:
        # designed to deal with sub folders archiving /some/folder/with/subfolders/there/*
        archives = []
        to_upload = []
        if candidate.name == "*":
            if not candidate.parent.exists():
                raise NotADirectoryError(
                    f"The directory you specified does not exist. '{candidate.parent}'"
                )
        else:
            if not candidate.exists():
                raise NotADirectoryError(
                    f"The directory you specified does not exist. '{candidate}'"
                )

        for file_or_folder in candidate.parent.iterdir():
            archive_destination = generate_archive_dest(
                file_or_folder, candidate.parent, tmp_dir_name
            )

            if file_or_folder.is_file():
                to_upload.append(file_or_folder)
                continue

            files_to_compress = list(glob_files(file_or_folder, pattern))

            if not files_to_compress:
                continue

            archive = compress(
                file_or_folder,
                archive_destination,
                files_to_compress,
            )
            archives.append(archive)

        return {"files": to_upload + archives, "created": archives}
