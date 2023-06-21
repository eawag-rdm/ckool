import pathlib
import os
import pytest

from erol.upload import upload_large_file

CKAN_URL = os.environ.get('CKAN_URL')
API_KEY = os.environ.get('CKAN_API')
PACKAGE_NAME = os.environ.get('TEST_PACKAGE_NAME')


@pytest.mark.slow_or_impure
def test_upload_small(remote_interface, tmp_path, small_file,):
    file = small_file

    response = upload_large_file(file, PACKAGE_NAME, CKAN_URL, API_KEY, "Dataset", "public")
    response.raise_for_status()


@pytest.mark.slow_or_impure
def test_upload_large(remote_interface, tmp_path, large_file):
    file = large_file

    response = upload_large_file(file, PACKAGE_NAME, CKAN_URL, API_KEY, "Dataset", "public")
    response.raise_for_status()


#@pytest.mark.slow_or_impure
def test_upload_very_large(remote_interface, tmp_path, very_large_file):
    file = very_large_file

    response = upload_large_file(file, PACKAGE_NAME, CKAN_URL, API_KEY, "Dataset", "public")
    response.raise_for_status()
