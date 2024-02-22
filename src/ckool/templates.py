import pathlib
from typing import Callable

from ckool import (
    EMPTY_FILE_NAME,
    HASH_BLOCK_SIZE,
    HASH_TYPE,
    TEMPORARY_DIRECTORY_NAME,
    UPLOAD_FUNC_FACTOR,
)
from ckool.ckan.ckan import CKAN
from ckool.interfaces.interfaces import SecureInterface
from ckool.other.caching import read_cache, update_cache
from ckool.other.config_parser import config_for_instance
from ckool.other.file_management import find_archive, iter_files, stats_file
from ckool.other.types import HashTypes
from ckool.other.utilities import DataIntegrityError, collect_metadata, upload_via_api


def upload_resource_file_via_api(
    ckan_api_input, package_name, filepath, metadata, progressbar, *args, **kwargs
):
    ckan_instance = CKAN(**ckan_api_input)
    return ckan_instance.create_resource_of_type_file(
        file=filepath, package_id=package_name, progressbar=progressbar, **metadata
    )


def upload_resource_link_via_api(
    ckan_api_input, package_name, metadata, *args, **kwargs
):
    ckan_instance = CKAN(**ckan_api_input)
    return ckan_instance.create_resource_of_type_link(
        package_id=package_name, **metadata
    )


def upload_resource_file_via_scp(
    ckan_api_input,
    secure_interface_input,
    ckan_storage_path,
    package_name,
    filepath,
    metadata,
    empty_file_name: str = EMPTY_FILE_NAME,
    progressbar: bool = True,
):
    si = SecureInterface(**secure_interface_input)

    # Upload empty resource (already using the correct metadata
    empty = filepath.parent / empty_file_name
    empty.touch()

    ckan_instance = CKAN(**ckan_api_input)
    ckan_instance.create_resource_of_type_file(
        file=empty, package_id=package_name, progressbar=False, **metadata
    )

    empty_file_location = ckan_instance.get_local_resource_path(
        package_name=package_name,
        resource_id_or_name=empty_file_name,
        ckan_storage_path=ckan_storage_path,
    )

    si.scp(
        local_filepath=filepath,
        remote_filepath=empty_file_location,
        progressbar=progressbar,
    )

    new_resource_name = name if (name := metadata.get("name")) else filepath.name
    return ckan_instance.patch_empty_resource_name(
        package_name=package_name, new_resource_name=new_resource_name
    )


def get_upload_func(
    file_sizes,
    space_available_on_server_root_disk,
    parallel_upload,
    factor: float = UPLOAD_FUNC_FACTOR,
    is_link: bool = False,
):
    if is_link:
        return upload_resource_link_via_api

    if upload_via_api(
        file_sizes=file_sizes,
        space_available_on_server_root_disk=space_available_on_server_root_disk,
        parallel_upload=parallel_upload,
        factor=factor,
    ):
        return upload_resource_file_via_api
    else:
        return upload_resource_file_via_scp


def hash_remote(
    ckan_api_input: dict,
    secure_interface_input: dict,
    ckan_storage_path: str,
    package_name: str,
    resource_id_or_name: str,
    hash_type: HashTypes | str = HASH_TYPE.sha256,
):
    hash_type_map = {  # mapping the input one might expect to linux command CKAN only supports md5 and sha256
        "md5": "md5sum",
        # "sha1": "sha1sum",
        # "sha224": "sha224sum",
        "sha256": "sha256sum",
        # "sha512": "sha512sum",
    }
    si = SecureInterface(**secure_interface_input)
    ckan = CKAN(**ckan_api_input)
    filepath = ckan.get_local_resource_path(
        package_name, resource_id_or_name, ckan_storage_path
    )
    out, err = si.ssh(
        f"{hash_type_map[hash_type if isinstance(hash_type, str) else hash_type.value]} {filepath}"
    )
    return out.split(" ")[0]


def resource_integrity_remote_intact(
    ckan_api_input: dict,
    secure_interface_input: dict,
    ckan_storage_path: str,
    package_name: str,
    resource_id_or_name: str,
):
    ckan = CKAN(**ckan_api_input)
    meta = ckan.get_resource_meta(
        package_name=package_name,
        resource_id_or_name=resource_id_or_name,
    )
    hash_local = meta["hash"]
    hash_remote_ = hash_remote(
        ckan_api_input,
        secure_interface_input,
        ckan_storage_path,
        package_name,
        resource_id_or_name,
        hash_type=meta["hashtype"],
    )
    if not hash_local == hash_remote_:
        raise DataIntegrityError(
            f"The local hash and the remote hash are different. The data integrity is compromised.\n"
            f"local hash: '{hash_local}'\n"
            f"remote hash: {hash_remote_}"
        )
    return hash_local == hash_remote_


def resource_integrity_between_ckan_instances_intact(
    ckan_api_input_1: dict,
    ckan_api_input_2: dict,
    package_name: str,
    resource_id_or_name: str,
):
    meta_1 = CKAN(**ckan_api_input_1).get_resource_meta(
        package_name=package_name,
        resource_id_or_name=resource_id_or_name,
    )
    meta_2 = CKAN(**ckan_api_input_2).get_resource_meta(
        package_name=package_name,
        resource_id_or_name=resource_id_or_name,
    )
    return meta_1["hash"] == meta_2["hash"]


def handle_file(
    file: pathlib.Path,
    hash_func: Callable,
    hash_algorithm: HashTypes = HASH_TYPE,
    tmp_dir_name: str = TEMPORARY_DIRECTORY_NAME,
    block_size: int = HASH_BLOCK_SIZE,
    progressbar: bool = True,
):
    if (cache_file := stats_file(file, tmp_dir_name)).exists():
        return
    hash_ = hash_func(
        filepath=file,
        block_size=block_size,
        progressbar=progressbar,
    )
    update_cache(collect_metadata(file, hash_, hash_algorithm), cache_file)


def archive_folder(folder: dict, compression_func: Callable, progressbar):
    archive = find_archive(folder["archive_destination"])
    if not archive:
        archive = compression_func(
            root_folder=folder["root_folder"],
            archive_destination=folder["archive_destination"],
            files=folder["files"],
            progressbar=progressbar,
        )
    return archive


def handle_folder(
    folder: dict,
    hash_func: Callable,
    compression_func: Callable,
    hash_algorithm: HashTypes = HASH_TYPE,
    tmp_dir_name: str = "",  # should be emtpy, as the archive filepath already contains the tmp dir name
    block_size: int = HASH_BLOCK_SIZE,
    progressbar: bool = True,
):
    archive = archive_folder(folder, compression_func, progressbar)
    handle_file(
        archive, hash_func, hash_algorithm, tmp_dir_name, block_size, progressbar
    )


def handle_upload(
    package_name: str,
    package_folder: pathlib.Path,
    config: dict,
    section: str,
    ckan_instance: str,
    verify: bool,
    parallel: bool,
    progressbar: bool,
):
    metadata_map = {
        cache_file.name: read_cache(cache_file)
        for cache_file in iter_files(
            package_folder / TEMPORARY_DIRECTORY_NAME,
            include_pattern=".json$",
            tmp_dir_to_ignore=None,
        )
    }

    sizes = [meta["size"] for meta in metadata_map.values()]

    cfg_other = config_for_instance(config[section]["other"], ckan_instance)
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})
    cfg_secure_interface = config_for_instance(
        config[section]["ckan_server"], ckan_instance
    )

    upload_func = get_upload_func(
        file_sizes=sizes,
        space_available_on_server_root_disk=cfg_other[
            "space_available_on_server_root_disk"
        ],
        parallel_upload=parallel,
        factor=UPLOAD_FUNC_FACTOR,
    )
    for meta in metadata_map.values():
        filepath = meta["file"]
        del meta["file"]

        upload_func(
            ckan_api_input=cfg_ckan_api,
            secure_interface_input=cfg_secure_interface,
            ckan_storage_path=cfg_other["ckan_storage_path"],
            package_name=package_name,
            filepath=filepath,
            metadata=meta,
            empty_file_name=EMPTY_FILE_NAME,
            progressbar=progressbar,
        )

    ckan_instance = CKAN(**cfg_ckan_api)
    ckan_instance.reorder_package_resources(package_name)
