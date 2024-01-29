import pytest

from ckool.ckan.upload import upload_resource


@pytest.mark.impure
def test_upload_small(ckan_instance, ckan_envvars, ckan_setup_data, small_file):
    response = upload_resource(
        small_file,
        ckan_envvars["test_package"],
        ckan_envvars["host"],
        ckan_envvars["token"],
        "Dataset",
        "public",
        allow_insecure=True,
    )
    response.raise_for_status()


@pytest.mark.impure
def test_upload_large(ckan_instance, ckan_envvars, ckan_setup_data, large_file):
    response = upload_resource(
        large_file,
        ckan_envvars["test_package"],
        ckan_envvars["host"],
        ckan_envvars["token"],
        "Dataset",
        "public",
        allow_insecure=True,
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
        "Dataset",
        "public",
        allow_insecure=True,
    )
    response.raise_for_status()
