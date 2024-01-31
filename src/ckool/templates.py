def _create_package(
    metadata_file: str,
    package_folder: str,
    ckan_instance: str,
    compression_type: str,
    include_pattern: str,
    exclude_pattern: str,
    hash_algorithm: str,
    parallel: bool,
    workers: int,
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
