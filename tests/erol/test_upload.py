import pathlib
import os
import pytest

from erol.upload import upload_large_file


@pytest.mark.slow_or_impure
def test_upload_small(remote_interface, tmp_path, small_file,):
    file = small_file

    ckan_url = os.environ.get('CKAN_URL')
    api_key = os.environ.get('CKAN_API')

    response = upload_large_file(file, "this-is-a-package", ckan_url, api_key, "Dataset", "public")
    response.raise_for_status()


@pytest.mark.slow_or_impure
def test_upload_large(remote_interface, tmp_path, large_file):
    file = large_file

    ckan_url = os.environ.get('CKAN_URL')
    api_key = os.environ.get('CKAN_API')

    response = upload_large_file(file, "this-is-a-package", ckan_url, api_key, "Dataset", "public")
    response.raise_for_status()


#@pytest.mark.slow_or_impure
def test_upload_very_large(remote_interface, tmp_path, very_large_file):
    file = very_large_file

    ckan_url = os.environ.get('CKAN_URL')
    api_key = os.environ.get('CKAN_API')

    response = upload_large_file(file, "this-is-a-package", ckan_url, api_key, "Dataset", "public")
    response.raise_for_status()
