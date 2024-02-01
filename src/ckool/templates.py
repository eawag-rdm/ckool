import pathlib


# TODO: Full pipeline packing of files plus the hasing should run parallel, currently upload will only start, when preparation is finished.
def _upload_package(
    metadata_file: str,
    package_folder: str,
    compression_type: str,
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

    config: dict,
    ckan_instance: str,
    verify: bool,
):
    print(locals())
    return


def _download_all_metadata(

    config: dict,
    ckan_instance: str,
    verify: bool,
):
    print(locals())
    return


def upload_data_package():
    # datapackage_checker could also be included here

    # local preparation - with file sizes and hashes

    # create new package and upload all resources
    # uploading data via scp or api (decided via file_size)

    # check hashsums on remote system
    pass


def upload_resource():
    # input can be folder -> that will be compressed, local preparation

    # uploading to existing package

    # check hashsums on remote system
    pass


def publish_package():
    # download package check data consistency

    # upload package to eric open

    # publish to datacite

    # update published package

    pass


def reserve_doi():
    # package as input

    # reserve doi
    # save metadata locally

    pass
