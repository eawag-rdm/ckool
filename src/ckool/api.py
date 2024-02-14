import pathlib

from ckool import HASH_BLOCK_SIZE, TEMPORARY_DIRECTORY
from ckool.ckan.ckan import CKAN
from ckool.other.caching import read_cache, update_cache
from ckool.other.config_parser import config_for_instance
from ckool.other.file_management import (
    find_archive,
    get_compression_func,
    iter_files,
    iter_package,
    stats_file,
)
from ckool.other.hashing import get_hash_func
from ckool.other.types import CompressionTypes
from ckool.other.utilities import collect_metadata
from ckool.templates import get_upload_func

# TODO: Full pipeline packing of files plus the hashing should run parallel,
#  currently upload will only start, when preparation is finished.


# TODO adding additional resource metadata fields how? Maybe via file
#  Resource should be in alphabetical order how to set the order?
def _upload_package(
    package_name: str,
    package_folder: str,
    include_sub_folders: bool,
    compression_type: CompressionTypes,
    include_pattern: str,
    exclude_pattern: str,
    hash_algorithm: str,  # todo, MUST BE an enum
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
                if (cache_file := stats_file(file, TEMPORARY_DIRECTORY)).exists():
                    continue
                hash_ = hash_func(
                    filepath=file,
                    block_size=HASH_BLOCK_SIZE,
                    progressbar=True,
                )
                update_cache(collect_metadata(file, hash_, hash_algorithm), cache_file)
            elif include_sub_folders and (
                folder := info["folder"]
            ):  # folders are archived and then hashed
                archive = find_archive(folder["archive_destination"])
                if not archive:
                    archive = compression_func(
                        root_folder=folder["root_folder"],
                        archive_destination=folder["archive_destination"],
                        files=folder["files"],
                        progressbar=True,
                    )
                if not (cache_file := stats_file(archive, "")).exists():
                    hash_ = hash_func(
                        filepath=archive,
                        block_size=HASH_BLOCK_SIZE,
                        progressbar=True,
                    )
                    update_cache(
                        collect_metadata(archive, hash_, hash_algorithm),
                        cache_file,
                    )
            else:
                raise ValueError(
                    f"This should not happen, the dictionary does not have the expected content: '{repr(info)}'"
                )

        # Iter cache files for uploading
        metadata_map = {
            cache_file.name: read_cache(cache_file)
            for cache_file in iter_files(
                package_folder / TEMPORARY_DIRECTORY,
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
            parallel_upload=False,
            factor=4.8,
        )
        for meta in metadata_map.values():
            upload_func(
                ckan_api_input=cfg_ckan_api,
                secure_interface_input=cfg_secure_interface,
                ckan_storage_path=cfg_other["ckan_storage_path"],
                package_name=package_name,
                filepath=meta["file"],
                metadata=meta,
                empty_file_name="empty_file.empty",
                progressbar=True,
            )
    else:
        raise NotImplementedError("Parallel will be implemented soon.")


def _upload_resource(
    package_name: str,
    filepath: str,
    hash_algorithm: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    section = "Production" if not test else "Test"

    filepath = pathlib.Path(filepath)
    hash_func = get_hash_func(hash_algorithm)

    if filepath.is_file():
        if not (cache_file := stats_file(filepath, TEMPORARY_DIRECTORY)).exists():
            hash_ = hash_func(
                filepath=filepath,
                block_size=HASH_BLOCK_SIZE,
                progressbar=True,
            )
            update_cache(
                metadata := collect_metadata(filepath, hash_, hash_algorithm),
                cache_file,
            )
    else:
        raise ValueError(
            f"The filepath your specified '{filepath.as_posix()}' is not a file."
        )

    cfg_other = config_for_instance(config[section]["other"], ckan_instance)
    cfg_ckan_api = config_for_instance(config[section]["ckan_api"], ckan_instance)
    cfg_ckan_api.update({"verify_certificate": verify})
    cfg_secure_interface = config_for_instance(
        config[section]["ckan_server"], ckan_instance
    )

    upload_func = get_upload_func(
        file_sizes=[metadata["size"]],
        space_available_on_server_root_disk=cfg_other[
            "space_available_on_server_root_disk"
        ],
        parallel_upload=False,
        factor=4.8,
    )

    upload_func(
        ckan_api_input=cfg_ckan_api,
        secure_interface_input=cfg_secure_interface,
        ckan_storage_path=cfg_other["ckan_storage_path"],
        package_name=package_name,
        filepath=metadata["file"],
        metadata=metadata,
        empty_file_name="empty_file.empty",
        progressbar=True,
    )
    return


def _prepare_package(
    package: pathlib.Path,
    include_sub_folders: bool,
    include_pattern: str,
    exclude_pattern: str,
    compression_type: CompressionTypes,
    hash_algorithm: str,
    parallel: bool,
    config: dict,
):
    """
    Example calls here
    """
    # prepare_for_upload_sequential
    # prepare_for_upload_parallel
    print(locals())
    return


def _download_package(
    package_name: str,
    destination: str,
    chunk_size: int,
    parallel: bool,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    """
    Example calls here
    """
    # check if files hash and size are available
    print(locals())
    return


def _download_resource(
    url: str,
    destination: str,
    name: str,
    chunk_size: int,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    """
    Example calls here
    """
    if name is None:
        name = pathlib.Path(url).name
    print(locals())
    return


def _download_resources(
    url_file: str,
    destination_folder: str,
    chunk_size: int,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    """
    Example calls here
    """
    if destination_folder is None:
        destination = pathlib.Path.cwd()
    print(locals())
    return


def _download_metadata(
    package_name: str,
    filter_fields: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    filter_fields = filter_fields.split(",")
    print(locals())
    return


def _download_all_metadata(
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    print(locals())
    return


def _patch_package(
    metadata_file: str,
    package_folder: str,
    parallel: bool,
    skip_prompt: bool,
    recollect_file_stats: bool,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    """should be interactive, show diff, let user select what to patch"""
    print(locals())
    return


def _patch_resource(
    metadata_file: str,
    file: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    print(locals())

    """should be interactive, show diff, let user select what to patch"""
    return


def _patch_metadata(
    metadata_file: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
    test: bool,
):
    print(locals())
    return


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
    track_progress: bool,
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
    # download package check data consistency

    # upload package to eric open

    # publish to datacite

    # update published package
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


def reserve_doi():
    # package as input

    # reserve doi
    # save metadata locally

    pass
