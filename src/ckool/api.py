import pathlib

from ckool import (
    DOWNLOAD_CHUNK_SIZE,
    HASH_BLOCK_SIZE,
    PACKAGE_META_DATA_FILE_ENDING,
    TEMPORARY_DIRECTORY_NAME,
)
from ckool.ckan.ckan import CKAN, filter_resources, get_resource_key
from ckool.ckan.publishing import (
    any_missing_organization_projects_variables,
    create_missing_organization_projects_variables,
    get_missing_organization_projects_variables,
    pre_publication_checks,
)
from ckool.datacite.doi_store import LocalDoiStore
from ckool.other.caching import read_cache, update_cache
from ckool.other.config_parser import config_for_instance
from ckool.other.file_management import get_compression_func, iter_package
from ckool.other.hashing import get_hash_func
from ckool.other.types import CompressionTypes, HashTypes
from ckool.parallel_runner import map_function_with_threadpool
from ckool.templates import handle_file, handle_folder, handle_upload, hash_remote


# TODO adding additional resource metadata fields how? Maybe via file
#  Resource should be in alphabetical order how to set the order?
def _upload_package(
    package_name: str,
    package_folder: str,
    include_sub_folders: bool,
    compression_type: CompressionTypes,
    include_pattern: str,
    exclude_pattern: str,
    hash_algorithm: HashTypes,
    parallel: bool,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    """
    Example calls here:

    """

    section = "Production" if not test else "Test"
    package_folder = pathlib.Path(package_folder)
    hash_func = get_hash_func(hash_algorithm)
    compression_func = get_compression_func(compression_type)

    if not parallel:
        for info in iter_package(
            package_folder,
            include_pattern=include_pattern,
            exclude_pattern=exclude_pattern,
        ):
            if file := info["file"]:  # files are hashed
                handle_file(
                    file,
                    hash_func,
                    hash_algorithm,
                    tmp_dir_name=TEMPORARY_DIRECTORY_NAME,
                    block_size=HASH_BLOCK_SIZE,
                    progressbar=True,
                )
            elif include_sub_folders and (
                folder := info["folder"]
            ):  # folders are archived and then hashed
                handle_folder(
                    folder,
                    hash_func,
                    compression_func,
                    hash_algorithm,
                    tmp_dir_name="",  # should be emtpy, as the archive filepath already contains the tmp dir name
                    block_size=HASH_BLOCK_SIZE,
                    progressbar=True,
                )
            else:
                raise ValueError(
                    f"This should not happen, the dictionary does not have the expected content: '{repr(info)}'"
                )

        handle_upload(
            package_name,
            package_folder,
            config,
            section,
            ckan_instance,
            verify,
            parallel,
            progressbar=True,
        )
    else:
        raise NotImplementedError("Parallel will be implemented soon.")


def _upload_resource(
    package_name: str,
    filepath: str,
    hash_algorithm: HashTypes,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    section = "Production" if not test else "Test"

    filepath = pathlib.Path(filepath)
    hash_func = get_hash_func(hash_algorithm)

    if filepath.is_file():
        handle_file(
            filepath,
            hash_func,
            hash_algorithm,
            tmp_dir_name=TEMPORARY_DIRECTORY_NAME,
            block_size=HASH_BLOCK_SIZE,
            progressbar=True,
        )

    else:
        raise ValueError(
            f"The filepath your specified '{filepath.as_posix()}' is not a file."
        )

    handle_upload(
        package_name,
        filepath.parent,
        config,
        section,
        ckan_instance,
        verify,
        parallel=False,
        progressbar=True,
    )


def _prepare_package(
    package_folder: str,
    include_sub_folders: bool,
    include_pattern: str,
    exclude_pattern: str,
    compression_type: CompressionTypes,
    hash_algorithm: HashTypes,
    parallel: bool,
    config: dict,
):
    package_folder = pathlib.Path(package_folder)
    hash_func = get_hash_func(hash_algorithm)
    compression_func = get_compression_func(compression_type)

    if not parallel:
        for info in iter_package(
            package_folder,
            include_pattern=include_pattern,
            exclude_pattern=exclude_pattern,
        ):
            if file := info["file"]:  # files are hashed
                handle_file(
                    file,
                    hash_func,
                    hash_algorithm,
                    tmp_dir_name=TEMPORARY_DIRECTORY_NAME,
                    block_size=HASH_BLOCK_SIZE,
                    progressbar=True,
                )
            elif include_sub_folders and (
                folder := info["folder"]
            ):  # folders are archived and then hashed
                handle_folder(
                    folder,
                    hash_func,
                    compression_func,
                    hash_algorithm,
                    tmp_dir_name="",  # should be emtpy, as the archive filepath already contains the tmp dir name
                    block_size=HASH_BLOCK_SIZE,
                    progressbar=True,
                )
            else:
                raise ValueError(
                    f"This should not happen, the dictionary does not have the expected content: '{repr(info)}'"
                )


def _download_package(
    package_name: str,
    destination: str,
    parallel: bool,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
    chunk_size: int = DOWNLOAD_CHUNK_SIZE,
):
    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})

    ckan = CKAN(**cfg_ckan_api)
    return ckan.download_package_with_resources(
        package_name=package_name,
        destination=destination,
        parallel=parallel,
        max_workers=None,
        chunk_size=chunk_size,
    )


def _download_resource(
    url: str,
    destination: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
    chunk_size: int = DOWNLOAD_CHUNK_SIZE,
):
    """
    Example calls here
    """
    destination = pathlib.Path(destination)
    if destination.exists():
        destination = destination / pathlib.Path(url).name

    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})

    ckan = CKAN(**cfg_ckan_api)
    ckan.download_resource(
        url=url,
        destination=destination,
        chunk_size=chunk_size,
    )

    return


def _download_resources(
    url_file: str,
    destination_folder: str,
    parallel: bool,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
    chunk_size: int = DOWNLOAD_CHUNK_SIZE,
):
    destination = pathlib.Path(destination_folder)
    url_file = pathlib.Path(url_file)
    with url_file.open("r") as urls:
        urls_to_download = [url for url in urls]

    if parallel:
        map_function_with_threadpool(
            _download_resource,
            [
                [
                    url,
                    destination.as_posix(),
                    config,
                    ckan_instance,
                    verify,
                    test,
                    chunk_size,
                ]
                for url in urls_to_download
            ],
        )
    else:
        for url in urls:
            _download_resource(
                url,
                destination.as_posix(),
                config,
                ckan_instance,
                verify,
                test,
                chunk_size,
            )


def _download_metadata(
    package_name: str,
    filter_fields: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})
    filter_fields = filter_fields.split(",")

    ckan = CKAN(**cfg_ckan_api)
    return ckan.get_package(package_name=package_name, filter_fields=filter_fields)


def _download_all_metadata(
    include_private,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})
    ckan = CKAN(**cfg_ckan_api)
    result = ckan.get_all_packages(include_private=include_private)
    if result["count"] == 1000:
        raise Warning(
            "The maximal numbers of rows were retrieved. "
            "Please check the ckanapi documentation for the package_search function "
            "on how to implement retrieval of more rows."
        )
    return result


def _patch_package(
    metadata_file: str,
    package_name: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    """
    should be interactive, show diff, let user select what to patch
    probably needs its own implementation independent of ckanapi
    """
    raise NotImplementedError("Parallel will be implemented soon.")
    # section = "Production" if not test else "Test"
    # cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    # cfg_ckan_api.update({"verify_certificate": verify})
    # ckan = CKAN(**cfg_ckan_api)
    # metadata_in_ckan = ckan.get_package(package_name)

    return


def _patch_resource(
    metadata_file: str,
    file: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    raise NotImplementedError("Parallel will be implemented soon.")
    """should be interactive, show diff, let user select what to patch"""
    return


def _patch_resource_hash(
    package_name: str,
    resource_name: str,
    local_resource_path: str,
    hash_algorithm: HashTypes,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    section = "Production" if not test else "Test"

    cfg_other = config_for_instance(config[section]["other"], ckan_instance)
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})
    cfg_secure_interface = config_for_instance(
        config[section]["ckan_server"], ckan_instance
    )

    ckan = CKAN(**cfg_ckan_api)

    hash_rem = hash_remote(
        ckan_api_input=cfg_ckan_api,
        secure_interface_input=cfg_secure_interface,
        ckan_storage_path=cfg_other["ckan_storage_path"],
        package_name=package_name,
        resource_name=resource_name,
        hash_type=hash_algorithm,
    )

    if local_resource_path is not None:
        local_resource_path = pathlib.Path(local_resource_path)
        hash_func = get_hash_func(hash_algorithm)
        hash_loc = hash_func(local_resource_path)
        if hash_func != hash_loc:
            raise ValueError(
                f"The local file '{local_resource_path.as_posix()}' and the remote file do not have the same hash!\n"
                f"local hash: '{hash_loc}'\n"
                f"remote hash: '{hash_rem}'"
            )

    data = ckan.get_package(package_name)["resources"]
    key = get_resource_key(data, package_name, resource_name)
    if key == "id":
        resource_id = resource_name
    else:
        resource_id = [d["id"] for d in data if d[key] == resource_name][0]

    ckan.patch_resource_metadata(
        resource_id=resource_id,
        resource_data_to_update={"hash": hash_rem, "hashtype": hash_algorithm.value},
    )


def _patch_metadata(
    metadata_file: str,
    package_name: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    """probably needs its own implementation independent of ckanapi"""
    metadata_file = pathlib.Path(metadata_file)

    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})

    metadata = read_cache(metadata_file)

    ckan = CKAN(**cfg_ckan_api)
    return ckan.patch_package_metadata(package_name, metadata)


def _patch_datacite(
    metadata_file: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    print(locals())
    return


def _publish_package(
    package_name: str,
    check_data_integrity: bool,
    create_missing_: bool,
    exclude_resources: str,
    parallel: bool,
    ckan_instance_destination: str,
    config: dict,
    ckan_instance_source: str,
    verify: bool,
    test: bool,
):
    exclude_resources = exclude_resources.split(",")
    (cwd := pathlib.Path.cwd() / TEMPORARY_DIRECTORY_NAME / package_name).mkdir(
        exist_ok=True
    )

    section = "Production" if not test else "Test"
    local_doi_store = config[section]["local_doi_store_path"]
    instances = [i["instance"] for i in config[section]["ckan_api"]]

    if ckan_instance_destination is None:
        if len(instances) > 2 or len(instances) == 1:
            raise ValueError(
                f"Your configuration file '{config['config_file_location']}' "
                f"contains more than 2 or less that 1 resources:\n{repr(instances)}."
            )
        instances.remove(ckan_instance_source)
        ckan_instance_destination = instances[0]

    doi = LocalDoiStore(local_doi_store).get_doi_from_package(package_name)

    cfg_ckan_source = config_for_instance(
        config[section]["ckan_api"], ckan_instance_source
    )
    cfg_ckan_source.update({"verify_certificate": verify})

    cfg_ckan_destination = config_for_instance(
        config[section]["ckan_api"], ckan_instance_destination
    )
    cfg_ckan_destination.update({"verify_certificate": verify})

    ckan_source = CKAN(**cfg_ckan_source)
    ckan_destination = CKAN(**cfg_ckan_destination)

    metadata = ckan_source.get_package(package_name=package_name)

    metadata_filtered = filter_resources(
        metadata,
        resources_to_exclude=exclude_resources,
        always_to_exclude_restriction_levels=["only_allowed_users"],
    )
    # TODO: Must add check for .json.meta file in tmp dir, if it exists, the upload procedure is very different.
    package_metadata_file = (cwd / package_name).with_suffix(
        PACKAGE_META_DATA_FILE_ENDING
    )
    update_cache(metadata_filtered, cache_file=package_metadata_file)

    temporary_resource_names = {}
    if not parallel:
        for resource in metadata_filtered["resources"]:
            url = resource["url"]
            id_ = resource["id"]
            temporary_resource_names[id_] = f"{id_}-{pathlib.Path(url).name}"
            downloaded_file = ckan_source.download_resource(
                url=url, destination=(cwd / temporary_resource_names[id_])
            )
            if check_data_integrity:
                if not resource["hash"]:
                    raise ValueError(f"No hash value for resource '{url}' on CKAN!")

                hash_func = get_hash_func(resource["hashtype"])
                hash_local = hash_func(downloaded_file)
                if hash_local != resource["hash"]:
                    raise ValueError(
                        f"Something went wrong. The hash value '{hash_local}' ('{resource['hashtype']}') of the "
                        f"downloaded resource '{temporary_resource_names}' does not match the one "
                        f"on CKAN '{resource['hash']}'."
                    )

        # run checks
        existing_and_missing_entities = pre_publication_checks(
            ckan_instance_destination=ckan_destination,
            package_metadata=metadata_filtered,
        )

        if create_missing_:
            create_missing_organization_projects_variables(
                cfg_ckan_destination, metadata_filtered, existing_and_missing_entities
            )

        elif any_missing_organization_projects_variables(existing_and_missing_entities):
            missing = get_missing_organization_projects_variables(
                existing_and_missing_entities
            )
            raise ValueError(
                f"Publication can not continue. These entities are missing: {repr(missing)}"
            )

    # All these resources are intact Questions ( Should the resources always be downloaded again or should there be a hash_flag)

    # upload package to eric open

    # publish to datacite

    # update published package
    print(locals())
    pass


def _publish_organization(
    organization_name: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    # download package check data consistency

    # upload package to eric open

    # publish to datacite

    # update published package
    print(locals())
    pass


def _publish_controlled_vocabulary(
    organization_name: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    print(locals())
    pass


def _delete_package(
    package_name: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    section = "Production" if not test else "Test"
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})
    ckan = CKAN(**cfg_ckan_api)
    ckan.delete_package(package_id=package_name)
