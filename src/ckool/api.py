import pathlib

from ckool import DOWNLOAD_CHUNK_SIZE, HASH_BLOCK_SIZE, TEMPORARY_DIRECTORY_NAME
from ckool.ckan.ckan import CKAN
from ckool.other.caching import read_cache
from ckool.other.config_parser import config_for_instance
from ckool.other.file_management import get_compression_func, iter_package
from ckool.other.hashing import get_hash_func
from ckool.other.types import CompressionTypes, HashTypes
from ckool.parallel_runner import map_function_with_threadpool
from ckool.templates import handle_file, handle_folder, handle_upload


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
    exclude_resources: list,
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
