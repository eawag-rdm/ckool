import pathlib

import pytest

from erol.upload import upload_large_file


@pytest.mark.skip
def test_upload_large(remote_interface, tmp_path):
    huge_file = pathlib.Path("/home/horst/huge_file.bin")

    ckan_url = "http://159.89.215.168"
    api_key = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiIxUVRGZk9ub3F0QzNkQmNVUE1xWUhoVzNoc3ZjbnRuSjFxU1ZoUkdsMVJjIiwiaWF0IjoxNjc5OTA5NzY3fQ.C-FjyA4UJbs2Z0Fbtv5aRW9DsgE0QGygxiWgNYtfQwk"

    response = upload_large_file(huge_file, "this-is-a-package", ckan_url, api_key)
