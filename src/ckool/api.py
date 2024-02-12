import pathlib

from ckool.other.types import CompressionTypes


# TODO: Full pipeline packing of files plus the hasing should run parallel,
#  currently upload will only start, when preparation is finished.
def _upload_package(
    metadata_file: str,
    package_folder: str,
    compression_type: CompressionTypes,
    include_pattern: str,
    exclude_pattern: str,
    hash_algorithm: str,
    parallel: bool,
    workers: int,
    config: dict,
    ckan_instance: str,
    verify: bool,
):
    """
    Example calls here:

    """
    print(locals())
    return


def _upload_resource(
    metadata_file: str,
    file: str,
    config: dict,
    ckan_instance: str,
    verify: bool,
):
    """
    Example calls here
    """
    # check if files hash and size are available
    print(locals())
    return


def _prepare_package(
    package: pathlib.Path,
    include_pattern: str,
    exclude_pattern: str,
    compression_type: CompressionTypes,
    hash_algorithm: str,
    parallel: bool,
    workers: int,
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
    workers: int,
    config: dict,
    ckan_instance: str,
    verify: bool,
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
):
    filter_fields = filter_fields.split(",")
    print(locals())
    return


def _download_all_metadata(
    config: dict,
    ckan_instance: str,
    verify: bool,
):
    print(locals())
    return


def _patch_package(
    metadata_file: str,
    package_folder: str,
    parallel: bool,
    skip_prompt: bool,
    recollect_file_stats: bool,
):
    """should be interactive, show diff, let user select what to patch"""
    print(locals())
    return


def _patch_resource(
    metadata_file: str, file: str, config: dict, ckan_instance: str, verify: bool
):
    print(locals())

    """should be interactive, show diff, let user select what to patch"""
    return


def _patch_metadata(metadata_file: str, config: dict, ckan_instance: str, verify: bool):
    print(locals())
    return


def _patch_datacite(metadata_file: str, config: dict, ckan_instance: str, verify: bool):
    print(locals())
    return


def _publish_package(
    package_name: str,
    check_data_integrity: bool,
    track_progress: bool,
    config: dict,
    ckan_instance: str,
    verify: bool,
):
    # download package check data consistency

    # upload package to eric open

    # publish to datacite

    # update published package
    print(locals())
    pass


def reserve_doi():
    # package as input

    # reserve doi
    # save metadata locally

    pass
