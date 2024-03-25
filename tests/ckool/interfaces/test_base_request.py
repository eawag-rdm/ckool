import pytest

from ckool.interfaces.base_request import base_get, base_post


@pytest.mark.impure
def test_base_get(ckan_envvars):
    get = base_get(ckan_envvars["host"], ckan_envvars["token"])
    response = get("/api/3/action/package_list", verify=False)
    assert response["success"]


#@pytest.mark.impure
def test_base_post(ckan_setup_data, ckan_envvars, ckan_entities, tmp_path):
    test_file = tmp_path / "test_file.txt"
    with test_file.open("w+") as f:
        f.write("test")

    post = base_post(ckan_envvars["host"], ckan_envvars["token"])
    response = post(
        "/api/3/action/resource_create",
        {
            "package_id": ckan_entities["test_package"],
            "name": "pytest_resource",
            "resource_type": "Dataset",
            "restricted_level": "public",
            "format": "txt",
            "size": "1024",
        },
        files={"upload": (test_file.name, test_file.open("rb").read())},
        verify=False,
    )
    assert response["success"]
