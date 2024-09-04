import pathlib
import sys
import tempfile
from copy import deepcopy
from typing import Callable

import paramiko

from ckool import (
    HASH_BLOCK_SIZE,
    HASH_TYPE,
    LOGGER,
    PACKAGE_META_DATA_FILE_ENDING,
    PUBLICATION_INTEGRITY_CHECK_CACHE,
    TEMPORARY_DIRECTORY_NAME,
    UPLOAD_FUNC_FACTOR,
    UPLOAD_IN_PROGRESS_STRING,
)
from ckool.ckan.ckan import CKAN, filter_resources
from ckool.ckan.publishing import (
    any_missing_organization_projects_variables,
    collect_missing_entity,
    create_missing_organization_projects_variables,
    create_resource_raw,
    get_missing_organization_projects_variables,
    pre_publication_checks,
)
from ckool.interfaces.interfaces import SecureInterface
from ckool.other.caching import read_cache, update_cache
from ckool.other.config_parser import config_for_instance
from ckool.other.file_management import (
    find_archive,
    get_compression_func,
    iter_files,
    stats_file,
)
from ckool.other.hashing import get_hash_func
from ckool.other.types import CompressionTypes, HashTypes
from ckool.other.utilities import (
    collect_metadata,
    extract_resource_id_and_name,
    resource_is_link,
    upload_via_api,
)


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
    progressbar: bool = True,
):
    if isinstance(filepath, str):
        filepath = pathlib.Path(filepath)

    real_hash = metadata["hash"]
    metadata["hash"] = UPLOAD_IN_PROGRESS_STRING

    with tempfile.TemporaryDirectory() as tmp:
        tmp = pathlib.Path(tmp)
        empty = tmp / filepath.name
        empty.touch()

        ckan_instance = CKAN(**ckan_api_input)
        ckan_instance.create_resource_of_type_file(
            file=empty, package_id=package_name, progressbar=False, **metadata
        )
        empty.unlink()

    if not (resource_name := metadata.get("name")):
        resource_name = filepath.name

    empty_file_location = ckan_instance.get_local_resource_path(
        package_name=package_name,
        resource_id_or_name=resource_name,
        ckan_storage_path=ckan_storage_path,
    )

    si = SecureInterface(**secure_interface_input)
    si.scp(
        local_filepath=filepath,
        remote_filepath=empty_file_location,
        progressbar=progressbar,
    )

    resource_id = ckan_instance.resolve_resource_id_or_name_to_id(
        package_name=package_name,
        resource_id_or_name=extract_resource_id_and_name(filepath.name)["name"],
    )["id"]

    ckan_instance.patch_resource_metadata(
        resource_id=resource_id, resource_data_to_update={"hash": real_hash}
    )


def get_upload_func(
    file_sizes,
    space_available_on_server_root_disk,
    parallel_upload,
    factor: float = UPLOAD_FUNC_FACTOR,
    is_link: bool = False,
    force_scp: bool = False,
):
    if is_link:
        return upload_resource_link_via_api

    if force_scp:
        return upload_resource_file_via_scp

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
    hashtype: HashTypes | str = HASH_TYPE.sha256,
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
        f"{hash_type_map[hashtype if isinstance(hashtype, str) else hashtype.value]} {filepath}"
    )

    if err:
        raise paramiko.ssh_exception.SSHException(
            f"An error occurred while hashing resource '{resource_id_or_name}'.\n{err}"
        )

    return out.split(" ")[0]


def hash_all_resources(
    package_name: str,
    ckan_api_input: dict,
    secure_interface_input: dict,
    ckan_storage_path: str,
    hashtype: HashTypes | str = HASH_TYPE.sha256,
    only_if_hash_missing: bool = True,
):
    ckan = CKAN(**ckan_api_input)
    resources = ckan.get_package(package_name)["resources"]
    for resource in resources:
        if resource_is_link(resource):
            LOGGER.info(f"Skipping resource '{resource['name']}', as it's a link...")
            continue

        if only_if_hash_missing:
            if resource["hash"] and resource.get("hashtype", False):
                continue

            LOGGER.info(
                f"Resource '{resource['name']}' has no hash and/or hashtype specified. Hashing now..."
            )
            LOGGER.info(f"... hashing '{resource['name']}'.")
            hash_ = hash_remote(
                ckan_api_input,
                secure_interface_input,
                ckan_storage_path,
                package_name,
                resource["id"],
                hashtype,
            )

            ckan.patch_resource_metadata(
                resource_id=resource["id"],
                resource_data_to_update={"hash": hash_, "hashtype": hashtype.value},
            )
        elif not only_if_hash_missing:
            LOGGER.info(f"... hashing '{resource['name']}'.")
            hash_ = hash_remote(
                ckan_api_input,
                secure_interface_input,
                ckan_storage_path,
                package_name,
                resource["id"],
                hashtype,
            )

            ckan.patch_resource_metadata(
                resource_id=resource["id"],
                resource_data_to_update={"hash": hash_, "hashtype": hashtype.value},
            )
        else:
            raise ValueError("Ooops this is unexpected, ")


def resource_integrity_remote_intact(
    ckan_api_input: dict,
    secure_interface_input: dict,
    ckan_storage_path: str,
    package_name: str,
    resource_id_or_name: str,
    cache_directory: pathlib.Path,
):
    hash_remote_ = None
    cf = {}

    LOGGER.info(f"... checking resource integrity for '{resource_id_or_name}'.")
    ckan = CKAN(**ckan_api_input)
    LOGGER.info("... retrieving hash value from CKAN.")

    meta = ckan.get_resource_meta(
        package_name=package_name,
        resource_id_or_name=resource_id_or_name,
    )
    hash_local = meta["hash"]

    integrity_cache_file = cache_directory / PUBLICATION_INTEGRITY_CHECK_CACHE
    temporary_resource_name = f"{meta['id']}-{meta['name']}"
    integrity_cache_key = f"remote-{temporary_resource_name}"
    if integrity_cache_file.exists():
        cf = read_cache(integrity_cache_file)
        hash_remote_ = cf.get(integrity_cache_key, None)

    if hash_remote_ is None:
        LOGGER.info(f"... hashing resource '{meta['name']}' remotely.")
        hash_remote_ = hash_remote(
            ckan_api_input,
            secure_interface_input,
            ckan_storage_path,
            package_name,
            resource_id_or_name,
            hashtype=meta["hashtype"],
        )
    else:
        LOGGER.info(f"... using cached remote hash for resource '{meta['name']}'.")

    if not hash_local == hash_remote_:
        LOGGER.warn(
            f"The local hash and the remote hash are different. The data integrity is compromised.\n"
            f"local hash: '{hash_local}'\n"
            f"remote hash: {hash_remote_}"
            "\nUpdating the resource hash on CKAN. The resource will be re-uploaded, when running 'ckool' again."
        )
        ckan.patch_resource_metadata(
            resource_id=resource_id_or_name,
            resource_data_to_update={
                "hash": hash_remote_,
                "hashtype": meta["hashtype"],
            },
        )
        sys.exit()
    LOGGER.info("... resource integrity intact.")

    cf[integrity_cache_key] = hash_local
    update_cache(cf, integrity_cache_file)

    return hash_local == hash_remote_


def package_integrity_remote_intact(
    ckan_api_input: dict,
    secure_interface_input: dict,
    ckan_storage_path: str,
    package_name: str,
    cache_directory: pathlib.Path,
):
    LOGGER.info(f"... checking resource integrity for package '{package_name}'.")
    ckan = CKAN(**ckan_api_input)
    for resource in ckan.get_package(package_name)["resources"]:
        if resource_is_link(resource):
            continue

        intact = resource_integrity_remote_intact(
            ckan_api_input=ckan_api_input,
            secure_interface_input=secure_interface_input,
            ckan_storage_path=ckan_storage_path,
            package_name=package_name,
            resource_id_or_name=resource["name"],
            cache_directory=cache_directory,
        )
        if not intact:
            raise ValueError(
                f"Something must have gone wrong during the upload of resource '{resource['name']}'. "
                f"The hash on the server does not match the one in ckan."
            )


def resource_integrity_between_ckan_instances_intact(
    ckan_api_input_1: dict,
    ckan_api_input_2: dict,
    package_name: str,
    resource_id_or_name: str,
):
    LOGGER.info(
        f"... checking resource integrity between ckan instances for resource '{resource_id_or_name}'."
    )
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
        LOGGER.info(f"... cache file for '{file.name}' found. Skipping hashing")
        return cache_file

    hash_ = hash_func(
        filepath=file,
        block_size=block_size,
        progressbar=progressbar,
    )
    return update_cache(collect_metadata(file, hash_, hash_algorithm), cache_file)


def archive_folder(folder: dict, compression_func: Callable, progressbar):
    archive = find_archive(folder["archive_destination"])
    if archive:
        LOGGER.info(
            f"... archive for folder '{folder['root_folder']}' found. Skipping compression."
        )
        return archive
    return compression_func(
        root_folder=folder["root_folder"],
        archive_destination=folder["archive_destination"],
        files=folder["files"],
        progressbar=progressbar,
    )


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
    return handle_file(
        archive, hash_func, hash_algorithm, tmp_dir_name, block_size, progressbar
    )


def wrapped_upload(
    meta: dict,
    package_name: str,
    ckan_instance: CKAN,
    cfg_other: dict,
    cfg_ckan_api: dict,
    cfg_secure_interface: dict,
    upload_func: Callable,
    progressbar: bool,
):
    meta_copy = deepcopy(meta)
    status = "normal"
    filepath = pathlib.Path(meta["file"])
    del meta_copy["file"]

    # Check if resource with corresponding hash is already on ckan
    LOGGER.info(f"... uploading resource '{filepath.name}' to '{package_name}'.")
    if ckan_instance.resource_exists(
        package_name=package_name, resource_name=filepath.name
    ):
        LOGGER.info("... resource already exists.")
        meta_on_ckan = ckan_instance.get_resource_meta(
            package_name=package_name, resource_id_or_name=filepath.name
        )

        if meta_on_ckan["hash"] == meta_copy["hash"]:
            # Resource is already on ckan and was uploaded successfully, skip resource upload
            LOGGER.info(
                "... resource hash on CKAN matches the local hash, upload skipped."
            )
            status = "skipped"
            return {"id": meta_on_ckan["id"], "name": filepath.name, "status": status}

        else:  # meta_on_ckan["hash"] == UPLOAD_IN_PROGRESS_STRING:
            LOGGER.info(
                "... resource hash on CKAN does not match the local hash preparing to overwrite."
            )
            ckan_instance.delete_resource(
                resource_id=ckan_instance.resolve_resource_id_or_name_to_id(
                    package_name=package_name, resource_id_or_name=filepath.name
                )["id"]
            )
            status = "replaced"

    LOGGER.debug("...starting upload.")
    _ = upload_func(
        ckan_api_input=cfg_ckan_api,
        secure_interface_input=cfg_secure_interface,
        ckan_storage_path=cfg_other["ckan_storage_path"],
        package_name=package_name,
        filepath=filepath,
        metadata=meta_copy,
        progressbar=progressbar,
    )

    resource_id = ckan_instance.resolve_resource_id_or_name_to_id(
        package_name=package_name, resource_id_or_name=filepath.name
    )["id"]

    return {"id": resource_id, "name": filepath.name, "status": status}


def handle_upload_all(
    package_name: str,
    package_folder: pathlib.Path,
    config: dict,
    section: str,
    ckan_instance_name: str,
    verify: bool,
    parallel: bool,
    progressbar: bool,
    force_scp: bool,
):
    metadata_map = {  # find all cache files
        cache_file.name: read_cache(cache_file)
        for cache_file in iter_files(
            package_folder / TEMPORARY_DIRECTORY_NAME,
            include_pattern=".json$",
            tmp_dir_to_ignore=None,
        )
    }

    # Check if file specified in cache file exists
    metadata_map_filtered = {
        filename: content
        for filename, content in metadata_map.items()
        if pathlib.Path(content["file"]).exists()
    }

    sizes = [meta["size"] for meta in metadata_map_filtered.values()]

    cfg_other = config_for_instance(config[section]["other"], ckan_instance_name)
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance_name)
    cfg_ckan_api.update({"verify_certificate": verify})
    cfg_secure_interface = config_for_instance(
        config[section]["ckan_server"], ckan_instance_name
    )

    upload_func = get_upload_func(
        file_sizes=sizes,
        space_available_on_server_root_disk=cfg_other[
            "space_available_on_server_root_disk"
        ],
        parallel_upload=parallel,
        factor=UPLOAD_FUNC_FACTOR,
        force_scp=force_scp,
    )
    if "via_scp" in upload_func.__name__:
        LOGGER.info("... upload via SCP selected.")
    else:
        LOGGER.info("... upload via API selected.")

    ckan_instance = CKAN(**cfg_ckan_api)
    _uploaded = []
    for meta in metadata_map_filtered.values():
        _uploaded.append(
            wrapped_upload(
                meta=meta,
                package_name=package_name,
                ckan_instance=ckan_instance,
                cfg_other=cfg_other,
                cfg_ckan_api=cfg_ckan_api,
                cfg_secure_interface=cfg_secure_interface,
                upload_func=upload_func,
                progressbar=progressbar,
            )
        )

    ckan_instance.reorder_package_resources(package_name)
    return _uploaded


# TODO: abstract handle_upload_single and _all to share a common base
def handle_upload_single(
    metadata_file: str,
    package_name: str,
    config: dict,
    section: str,
    ckan_instance_name: str,
    verify: bool,
    progressbar: bool,
    force_scp: bool,
):
    cfg_other = config_for_instance(config[section]["other"], ckan_instance_name)
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance_name)
    cfg_ckan_api.update({"verify_certificate": verify})
    cfg_secure_interface = config_for_instance(
        config[section]["ckan_server"], ckan_instance_name
    )

    meta = read_cache(pathlib.Path(metadata_file))
    ckan_instance = CKAN(**cfg_ckan_api)
    upload_func = get_upload_func(
        file_sizes=[int(meta["size"])],
        space_available_on_server_root_disk=cfg_other[
            "space_available_on_server_root_disk"
        ],
        parallel_upload=False,
        factor=UPLOAD_FUNC_FACTOR,
        force_scp=force_scp,
    )

    return wrapped_upload(
        meta=meta,
        package_name=package_name,
        ckan_instance=ckan_instance,
        cfg_other=cfg_other,
        cfg_ckan_api=cfg_ckan_api,
        cfg_secure_interface=cfg_secure_interface,
        upload_func=upload_func,
        progressbar=progressbar,
    )


def handle_folder_file(
    info: dict,
    include_sub_folders: bool,
    compression_type: CompressionTypes,
    hash_algorithm: HashTypes,
    progressbar: bool,
):
    hash_func = get_hash_func(hash_algorithm)
    compression_func = get_compression_func(compression_type)

    if file := info["file"]:  # files are hashed
        return handle_file(
            file,
            hash_func,
            hash_algorithm,
            tmp_dir_name=TEMPORARY_DIRECTORY_NAME,
            block_size=HASH_BLOCK_SIZE,
            progressbar=progressbar,
        )

    elif folder := info["folder"]:  # folders are compressed and then hashed
        if include_sub_folders:
            return handle_folder(
                folder,
                hash_func,
                compression_func,
                hash_algorithm,
                tmp_dir_name="",  # should be emtpy, as the archive filepath already contains the tmp dir name
                block_size=HASH_BLOCK_SIZE,
                progressbar=progressbar,
            )
    else:
        raise ValueError(
            f"This should not happen, the dictionary does not have the expected content:\n{repr(info)}"
        )


def handle_folder_file_upload(
    info: dict,
    package_name: str,
    include_sub_folders: bool,
    compression_type: CompressionTypes,
    hash_algorithm: HashTypes,
    force_scp,
    progressbar: bool,
    config: dict,
    ckan_instance_name: str,
    verify: bool,
    section: str,
):
    cache_file = handle_folder_file(
        info,
        include_sub_folders,
        compression_type,
        hash_algorithm,
        progressbar,
    )

    if cache_file is not None:
        return handle_upload_single(
            metadata_file=cache_file,
            package_name=package_name,
            config=config,
            section=section,
            ckan_instance_name=ckan_instance_name,
            verify=verify,
            progressbar=progressbar,
            force_scp=force_scp,
        )


def retrieve_and_filter_source_metadata(
    ckan_source: CKAN,
    package_name: str,
    exclude_resources: list | None,
    cwd: pathlib.Path,
    package_metadata_suffix: str = PACKAGE_META_DATA_FILE_ENDING,
):
    if exclude_resources is None:
        exclude_resources = []

    metadata = ckan_source.get_package(package_name=package_name)

    metadata_filtered = filter_resources(
        metadata,
        resources_to_exclude=exclude_resources,
        always_to_exclude_restriction_levels=["only_allowed_users"],
    )

    package_metadata_file = (cwd / package_name).with_suffix(package_metadata_suffix)
    update_cache(metadata_filtered, cache_file=package_metadata_file)
    return metadata_filtered


def handle_missing_entities(
    ckan_source: CKAN,
    ckan_target: CKAN,
    cfg_other_target: dict,
    create_missing_: bool,
    metadata_filtered: dict,
    projects_to_publish: list = None,
):
    LOGGER.info(
        "... checking all entities necessary for publication exists on CKAN target instance."
    )
    existing_and_missing_entities = pre_publication_checks(
        ckan_instance_destination=ckan_target,
        package_metadata=metadata_filtered,
        projects_to_publish=projects_to_publish,
    )

    if create_missing_:
        if any_missing_organization_projects_variables(existing_and_missing_entities):
            for to_create in collect_missing_entity(
                ckan_source, existing_and_missing_entities
            ):
                LOGGER.info(f"... creating entity of type '{to_create['entity']}'.")
                create_missing_organization_projects_variables(
                    ckan_target,
                    **to_create,
                    org_data_manager=cfg_other_target["datamanager"],
                    prepare_for_publication=True,
                )

    elif any_missing_organization_projects_variables(existing_and_missing_entities):
        missing = get_missing_organization_projects_variables(
            existing_and_missing_entities
        )
        raise ValueError(
            f"Publication can not continue. These entities are missing: {repr(missing)}"
        )

    return existing_and_missing_entities


def handle_resource_download_with_integrity_check(
    cfg_ckan_source: dict,
    package_name: str,
    resource: dict,
    check_data_integrity: bool,
    cwd: pathlib.Path,
    re_download: bool = True,
):
    ckan_source = CKAN(**cfg_ckan_source)

    name = resource["name"]
    id_ = resource["id"]
    temporary_resource_name = f"{id_}-{name}"
    temporary_resource_path = cwd / temporary_resource_name
    integrity_cache_file = cwd / PUBLICATION_INTEGRITY_CHECK_CACHE
    integrity_cache_key = f"local-{temporary_resource_name}"
    if not temporary_resource_path.exists() or re_download:
        ckan_source.download_resource(
            package_name=package_name,
            resource_name=name,
            destination=temporary_resource_path,
        )
    if (
        check_data_integrity and resource["url_type"] == "upload"
    ):  # skipping link resources
        LOGGER.info(f"... running integrity check for '{resource['name']}'.")
        if not resource["hash"]:
            raise ValueError(f"No resource hash for '{resource['name']}'.")

        cf = {}
        if integrity_cache_file.exists():
            cf = read_cache(integrity_cache_file)

        if (hash_local := cf.get(integrity_cache_key)) and not re_download:
            LOGGER.info(
                f"... using cached local hash for resource '{resource['name']}'."
            )
        else:
            hash_func = get_hash_func(resource["hashtype"])
            hash_local = hash_func(temporary_resource_path)

        if hash_local != resource["hash"]:
            LOGGER.warn(
                f"Something went wrong. The hash value '{hash_local}' ('{resource['hashtype']}') of the "
                f"downloaded resource '{temporary_resource_name}' does not match the one "
                f"on CKAN '{resource['hash']}'. "
                f"Resource will be deleted locally and re-downloaded when running 'ckool' again."
            )
            temporary_resource_path.unlink()
            sys.exit()
        cf[integrity_cache_key] = hash_local
        update_cache(cf, integrity_cache_file)
    return {"id": id_, "name": temporary_resource_name}


def create_resource_raw_wrapped(
    cfg_ckan_target: dict,
    cfg_other_target: dict,
    cfg_secure_interface_destination: dict,
    filepath: pathlib.Path,
    resource: dict,
    package_name: str,
    force_scp: bool = False,
):
    upload_func = get_upload_func(
        file_sizes=[
            int("0" if resource["size"] is None else resource["size"])
        ],  # if link size will be set to "0"
        space_available_on_server_root_disk=cfg_other_target[
            "space_available_on_server_root_disk"
        ],
        parallel_upload=False,
        factor=UPLOAD_FUNC_FACTOR,
        is_link=resource_is_link(resource),
        force_scp=force_scp,
    )
    LOGGER.info(f"... creating resource '{filepath.name}'.")
    create_resource_raw(
        ckan_api_input=cfg_ckan_target,
        secure_interface_input=cfg_secure_interface_destination,
        ckan_storage_path=cfg_other_target["ckan_storage_path"],
        package_name=package_name,
        metadata=resource,
        file_path=filepath.as_posix(),
        upload_func=upload_func,
        progressbar=True,
        prepare_for_publication=True,
    )
