import json

import ckanapi
import pytest


def test_get_package_metadata(ckan_instance, ckan_test_package):
    data = ckan_instance.get_package(ckan_test_package)
    print(json.dumps(data, indent=2))
    assert isinstance(data, dict) and len(data) > 0


def test_get_package_metadata_package_does_not_exist(ckan_instance):
    with pytest.raises(ckanapi.errors.NotFound):
        ckan_instance.get_package("this-package-name-does-not-exist")


def test_get_package_metadata_filtered(ckan_instance, ckan_test_package):
    data = ckan_instance.get_package(
        ckan_test_package, filter_fields=["maintainer", "author"]
    )
    assert len(data) == 2


def test_update_package_metadata(ckan_instance, ckan_test_package):
    data = ckan_instance.get_package(ckan_test_package)

    new_message = "this field was changed"
    original_message = data["notes"]

    data["notes"] = new_message
    ckan_instance.update_package_metadata(data)

    changed_data = ckan_instance.get_package(ckan_test_package)
    assert changed_data["notes"] == new_message

    changed_data["notes"] = original_message
    ckan_instance.update_package_metadata(changed_data)

    data = ckan_instance.get_package(ckan_test_package)
    assert data["notes"] == original_message


def test_patch_package_metadata(ckan_instance, ckan_test_package):
    data = ckan_instance.get_package(ckan_test_package)
    original_message = data["notes"]
    new_message = "this field was changed"

    data = ckan_instance.patch_package_metadata(
        ckan_test_package, {"notes": new_message}
    )
    assert data["notes"] == new_message

    data = ckan_instance.patch_package_metadata(
        ckan_test_package, {"notes": original_message}
    )
    assert data["notes"] == original_message
