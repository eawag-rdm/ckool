import pytest

from ckool.upload import upload_large_resource


@pytest.mark.slow_or_impure
def test_upload_small(ckan_api, ckan_url, ckan_package_name, small_file):
    response = upload_large_resource(
        small_file, ckan_package_name, ckan_url, ckan_api, "Dataset", "public"
    )
    response.raise_for_status()


@pytest.mark.slow_or_impure
def test_upload_large(ckan_api, ckan_url, ckan_package_name, large_file):
    response = upload_large_resource(
        large_file, ckan_package_name, ckan_url, ckan_api, "Dataset", "public"
    )
    response.raise_for_status()


# @pytest.mark.slow_or_impure
def test_upload_very_large(ckan_api, ckan_url, ckan_package_name, very_large_file):
    response = upload_large_resource(
        very_large_file, ckan_package_name, ckan_url, ckan_api, "Dataset", "public"
    )
    response.raise_for_status()
