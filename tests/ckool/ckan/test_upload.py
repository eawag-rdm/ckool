import pytest

from ckool.ckan.upload import upload_resource
from ckool.other.hashing import get_hash_func

hasher = get_hash_func("sha256")


@pytest.mark.impure
def test_upload_small(ckan_instance, ckan_envvars, ckan_setup_data, small_file):
    response = upload_resource(
        small_file,
        ckan_envvars["test_package"],
        ckan_envvars["host"],
        ckan_envvars["token"],
        file_hash=hasher(small_file),
        file_size=small_file.stat().st_size,
        verify=False,
    )
    response.raise_for_status()


@pytest.mark.impure
def test_upload_small_all_args(
    ckan_instance, ckan_envvars, ckan_setup_data, small_file
):
    response = upload_resource(
        small_file,
        ckan_envvars["test_package"],
        ckan_envvars["host"],
        ckan_envvars["token"],
        file_hash=hasher(small_file),
        file_size=small_file.stat().st_size,
        citation="text",
        description="text",
        format=".bin",
        hashtype="sha256",
        resource_type="Dataset",
        restricted_level="public",
        state="active",
        verify=False,
    )
    response.raise_for_status()


@pytest.mark.impure
def test_upload_large(ckan_instance, ckan_envvars, ckan_setup_data, large_file):
    response = upload_resource(
        large_file,
        ckan_envvars["test_package"],
        ckan_envvars["host"],
        ckan_envvars["token"],
        file_hash=hasher(large_file),
        file_size=large_file.stat().st_size,
        verify=False,
    )
    response.raise_for_status()


@pytest.mark.slow
@pytest.mark.impure
def test_upload_very_large(
    ckan_instance, ckan_envvars, ckan_setup_data, very_large_file
):
    response = upload_resource(
        very_large_file,
        ckan_envvars["test_package"],
        ckan_envvars["host"],
        ckan_envvars["token"],
        file_hash=hasher(very_large_file),
        file_size=very_large_file.stat().st_size,
        verify=False,
    )
    response.raise_for_status()
