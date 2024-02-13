import pathlib

from tqdm.auto import tqdm

from ckool import COMPRESSION_TYPE, TEMPORARY_DIRECTORY
from ckool.ckan.ckan import CKAN
from ckool.interfaces.interfaces import SecureInterface
from ckool.other.caching import read_cache, update_cache
from ckool.other.file_management import (
    get_compression_func,
    iter_package_and_prepare_for_upload,
)
from ckool.other.hashing import get_hash_func
from ckool.other.utilities import DataIntegrityError, upload_via_api

compression_func = get_compression_func(COMPRESSION_TYPE)


def setup_progress_bar(
    files: list = None,
    archive_destination: pathlib.Path = None,
    filepath: pathlib.Path = None,
    block_size: int = None,
    position: int = None,
):
    """
    for_compressing = {"file_list": "", "archive_destination_name": "", "position": ""}
    for_hashing = {"filepath":"","block_size":"","position":""}
    for_uploading = {"filepath":"", "position":""}
    """
    if files and archive_destination and position:
        return tqdm(
            files,
            desc=f"Zipping {archive_destination.name}",
            position=position,
        )
    if filepath and block_size:
        return tqdm(
            total=int(filepath.stat().st_size / block_size) + 1,
            desc=f"Hashing {filepath.name}",
            position=position,
        )
    if filepath:
        return tqdm(
            total=filepath.stat().st_size,
            unit="B",
            unit_scale=True,
            desc=f"Uploading {filepath.name}",
            position=position,
        )


def collect_stats(tmp_dir_name, overwrite, hash_type, filepath, progressbar):
    if tmp_dir_name in filepath.as_posix():
        cache_file = filepath.with_suffix(filepath.suffix + ".json")
    else:
        cache_file = filepath.parent / tmp_dir_name / (filepath.name + ".json")

    if cache_file.exists() and not overwrite:
        pass
    else:
        stats = {
            "file": str(filepath),
            "hash": get_hash_func(hash_type)(
                filepath=filepath, progressbar=progressbar
            ),
            "hash_type": hash_type,
            "size": filepath.stat().st_size,
        }

        update_cache(stats, cache_file)
        return [filepath, cache_file]


def upload_resource_file_via_api(
    ckan_api_input, package_name, filepath, cache_file, progressbar, *args, **kwargs
):
    ckan_instance = CKAN(**ckan_api_input)
    stats = read_cache(cache_file)
    ckan_instance.create_resource_of_type_file(
        file=filepath,
        package_id=package_name,
        file_hash=stats["hash"],
        file_size=stats["size"],
        hash_type=stats["hash_type"],
        progressbar=progressbar,
    )


def upload_resource_file_via_scp(
    ckan_api_input,
    secure_interface_input,
    ckan_storage_path,
    package_name,
    filepath,
    cache_file,
    empty_file_name: str = "empty_file.empty",
    progressbar: bool = True,
):
    si = SecureInterface(**secure_interface_input)

    # Upload empty resource (already using the correct metadata
    empty = filepath.parent / empty_file_name
    empty.touch()

    ckan_instance = CKAN(**ckan_api_input)
    stats = read_cache(cache_file)
    ckan_instance.create_resource_of_type_file(
        file=empty,
        package_id=package_name,
        file_hash=stats["hash"],
        file_size=stats["size"],
        hash_type=stats["hash_type"],
        progressbar=False,
    )

    empty_file_location = ckan_instance.get_local_resource_path(
        package_name=package_name,
        resource_name=empty_file_name,
        ckan_storage_path=ckan_storage_path,
    )

    return si.scp(
        local_filepath=filepath,
        remote_filepath=empty_file_location,
        show_progress=progressbar,
    )


def get_upload_func(
    file_sizes, space_available_on_server_root_disk, parallel_upload, factor: int = 4.8
):
    if upload_via_api(**locals()):
        return upload_resource_file_via_api
    else:
        return upload_resource_file_via_scp


def hash_remote(
    ckan_api_input: dict,
    secure_interface_input: dict,
    ckan_storage_path: str,
    package_name: str,
    resource_name: str,
    hash_type: str = "sha256",
):
    hash_type_map = {  # mapping the input one might expect to linux command
        "md5": "md5sum",
        "sha": "shasum",
        "sha1": "sha1sum",
        "sha224": "sha224sum",
        "sha256": "sha256sum",
        "sha386": "sha384sum",
        "sha512": "sha512sum",
    }
    si = SecureInterface(**secure_interface_input)
    ckan = CKAN(**ckan_api_input)
    filepath = ckan.get_local_resource_path(
        package_name, resource_name, ckan_storage_path
    )
    out, err = si.ssh(f"{hash_type_map.get(hash_type)} {filepath}")
    return out.split(" ")[0]


def check_uploaded_resource_integrity(
    ckan_api_input: dict,
    secure_interface_input: dict,
    ckan_storage_path: str,
    package_name: str,
    resource_name: str,
):
    ckan = CKAN(**ckan_api_input)
    meta = ckan.get_resource_meta(
        package_name=package_name,
        resource_name=resource_name,
    )
    hash_local = meta["hash"]
    hash_remote_ = hash_remote(
        ckan_api_input,
        secure_interface_input,
        ckan_storage_path,
        package_name,
        resource_name,
        hash_type=meta["hash_type"],
    )
    if not hash_local == hash_remote_:
        raise DataIntegrityError(
            f"The local hash and the remote hash are different. The data integrity is compromised.\n"
            f"local hash: '{hash_local}'\n"
            f"remote hash: {hash_remote_}"
        )


def build_start_conditions_for_parallel_runner(
    package_dir, tmp_dir_name, overwrite, hash_type
):
    start_conditions = []
    for static_or_dynamic in iter_package_and_prepare_for_upload(
        package_dir,
        None,
        None,
        compression_func,
        TEMPORARY_DIRECTORY,
    ):
        if dynamic := static_or_dynamic.get("dynamic"):
            dynamic["kwargs"].update({"progressbar": False})
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
