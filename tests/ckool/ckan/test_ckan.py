import json

import ckanapi
import pytest


@pytest.mark.slow_or_impure
def _delete_resource_ids_and_times(data: dict):
    """Due to setup and teardown for each test the times and ids are always different"""
    del data["id"]
    del data["metadata_created"]
    del data["metadata_modified"]
    del data["organization"]["created"]
    del data["organization"]["id"]
    del data["owner_org"]
    for i in range(len(data["resources"])):
        del data["resources"][i]["created"]
        del data["resources"][i]["id"]
        del data["resources"][i]["metadata_modified"]
        del data["resources"][i]["package_id"]
    return data


@pytest.mark.slow_or_impure
def test_get_all_packages(ckan_instance):
    assert ckan_instance.get_all_packages()


@pytest.mark.slow_or_impure
def test_get_package(ckan_instance, ckan_envvars, ckan_setup_data, valid_outputs):
    data = ckan_instance.get_package(ckan_envvars["test_package"])
    with (valid_outputs / "package_data_from_ckan.json").open() as f:
        right_data = json.load(f)
    assert _delete_resource_ids_and_times(data) == _delete_resource_ids_and_times(
        right_data
    )


@pytest.mark.slow_or_impure
def test_get_package_metadata_package_does_not_exist(ckan_instance, ckan_setup_data):
    with pytest.raises(ckanapi.errors.NotFound):
        ckan_instance.get_package("this-package-name-does-not-exist")


@pytest.mark.slow_or_impure
def test_get_package_metadata_filtered(ckan_instance, ckan_envvars, ckan_setup_data):
    data = ckan_instance.get_package(
        ckan_envvars["test_package"], filter_fields=["maintainer", "author"]
    )
    assert len(data) == 2


@pytest.mark.slow_or_impure
def test_update_package_metadata(ckan_instance, ckan_envvars, ckan_setup_data):
    data = ckan_instance.get_package(ckan_envvars["test_package"])

    new_message = "this field was changed"
    original_message = data["notes"]

    data["notes"] = new_message
    ckan_instance.update_package_metadata(data)

    changed_data = ckan_instance.get_package(ckan_envvars["test_package"])
    assert changed_data["notes"] == new_message

    changed_data["notes"] = original_message
    ckan_instance.update_package_metadata(changed_data)

    data = ckan_instance.get_package(ckan_envvars["test_package"])
    assert data["notes"] == original_message


@pytest.mark.slow_or_impure
def test_patch_package_metadata(ckan_instance, ckan_envvars, ckan_setup_data):
    data = ckan_instance.get_package(ckan_envvars["test_package"])
    original_message = data["notes"]
    new_message = "this field was changed"

    data = ckan_instance.patch_package_metadata(
        ckan_envvars["test_package"], {"notes": new_message}
    )
    assert data["notes"] == new_message

    data = ckan_instance.patch_package_metadata(
        ckan_envvars["test_package"], {"notes": original_message}
    )
    assert data["notes"] == original_message
