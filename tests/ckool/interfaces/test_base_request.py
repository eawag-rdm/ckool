import pytest

from ckool.interfaces.base_request import base_get, base_post


@pytest.mark.slow_or_impure
def test_base_get(ckan_url, ckan_api):
    get = base_get(ckan_url, ckan_api)
    assert get("/api/3/action/package_list")["success"]


@pytest.mark.slow_or_impure
def test_base_post(ckan_url, ckan_api, tmp_path):
    test_file = tmp_path / "test_file.txt"
    with test_file.open("w+") as f:
        f.write("test")

    post = base_post(ckan_url, ckan_api)
    response = post(
        "/api/3/action/resource_create",
        {
            "package_id": "test_package",
            "name": "pytest_resource",
            "resource_type": "Dataset",
            "restricted_level": "public",
            "format": "txt",
            "size": "1024",
        },
        files={"upload": (test_file.name, test_file.open("rb").read())},
    )
    print(response)
