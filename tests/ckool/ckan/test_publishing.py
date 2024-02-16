import pytest

from ckool.ckan.publishing import pre_publication_checks


@pytest.mark.impure
def test_pre_publication_checks_all_exist(ckan_instance, ckan_envvars, ckan_setup_data):
    package_metadata = ckan_instance.get_package(ckan_envvars["test_package"])
    result = pre_publication_checks(
        ckan_instance_destination=ckan_instance,
        package_metadata=package_metadata,
    )
    assert result == {
        "missing": {
            "package": [],
            "organization": [],
            "resources": [],
            "projects": [],
            "variables": [],
        },
        "exist": {
            "package": [ckan_envvars["test_package"]],
            "organization": [ckan_envvars["test_organization"]],
            "resources": [ckan_envvars["test_resource"]],
            "projects": [],
            "variables": [],
        },
    }
    ckan_instance.add_package_to_project(
        ckan_envvars["test_package"], ckan_envvars["test_project"]
    )
    package_metadata = ckan_instance.get_package(ckan_envvars["test_package"])
    result = pre_publication_checks(
        ckan_instance_destination=ckan_instance, package_metadata=package_metadata
    )

    assert result == {
        "missing": {
            "package": [],
            "organization": [],
            "resources": [],
            "projects": [],
            "variables": [],
        },
        "exist": {
            "package": [ckan_envvars["test_package"]],
            "organization": [ckan_envvars["test_organization"]],
            "resources": [ckan_envvars["test_resource"]],
            "projects": [ckan_envvars["test_project"]],
            "variables": [],
        },
    }
