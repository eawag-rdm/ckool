import pytest

from ckool.ckan.publishing import (
    any_missing_organization_projects_variables,
    collect_missing_entity,
    create_missing_organization_projects_variables,
    create_organization_raw,
    create_package_raw,
    create_project_raw,
    create_resource_raw,
    pre_publication_checks,
)
from ckool.other.utilities import resource_is_link
from ckool.templates import upload_resource_file_via_api, upload_resource_link_via_api


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


@pytest.mark.impure
def test_pre_publication_checks_none_exist(
    ckan_instance, ckan_envvars, ckan_setup_data
):
    ckan_instance.add_package_to_project(
        ckan_envvars["test_package"], ckan_envvars["test_project"]
    )
    package_metadata = ckan_instance.get_package(ckan_envvars["test_package"])

    package_metadata["name"] = "non-existent-package"
    package_metadata["organization"]["name"] = "non-existent-organization"
    package_metadata["groups"][0]["name"] = "non-existent-group"
    package_metadata["resources"][0]["name"] = "non-existent-resource"

    result = pre_publication_checks(
        ckan_instance_destination=ckan_instance,
        package_metadata=package_metadata,
    )
    assert result == {
        "missing": {
            "package": ["non-existent-package"],
            "organization": ["non-existent-organization"],
            "resources": ["non-existent-resource"],
            "projects": ["non-existent-group"],
            "variables": [],
        },
        "exist": {
            "package": [],
            "organization": [],
            "resources": [],
            "projects": [],
            "variables": [],
        },
    }


def test_any_missing_organization_projects_variables():
    assert any_missing_organization_projects_variables(
        {
            "missing": {
                "package": [],
                "organization": [],
                "resources": [],
                "projects": [],
                "variables": ["non-existent-variable"],
            },
            "exist": {...},
        }
    )

    assert any_missing_organization_projects_variables(
        {
            "missing": {
                "package": [],
                "organization": ["non-existent-organization"],
                "resources": [],
                "projects": [],
                "variables": [],
            },
            "exist": {...},
        }
    )

    assert any_missing_organization_projects_variables(
        {
            "missing": {
                "package": [],
                "organization": [],
                "resources": [],
                "projects": ["non-existent-group"],
                "variables": [],
            },
            "exist": {...},
        }
    )


@pytest.mark.impure
def test_create_missing_organization(ckan_instance, ckan_envvars, ckan_setup_data):
    new_org_name = "new_test_organization"
    org = {
        "approval_status": "approved",
        "created": "2024-02-19T14:32:30.816151",
        "datamanager": "ckan_admin",
        "description": "This is my organization.",
        "display_name": "Test_Organization",
        "homepage": "https://www.eawag.ch/de/",
        "id": "e82850c4-aca0-4060-ab01-486ad305fb10",
        "image_display_url": "https://www.techrepublic.com/wp-content/uploads/2017/03/meme05.jpg",
        "image_url": "https://www.techrepublic.com/wp-content/uploads/2017/03/meme05.jpg",
        "is_organization": True,
        "name": new_org_name,
        "num_followers": 0,
        "package_count": 1,
        "state": "active",
        "title": "Test_Organization",
        "type": "organization",
        "users": [
            {
                "about": None,
                "activity_streams_email_notifications": False,
                "capacity": "admin",
                "created": "2024-01-25T09:33:37.949157",
                "display_name": "ckan_admin",
                "email_hash": "7c512b13badc48258d94ef72d1c8889a",
                "fullname": None,
                "id": "a60585d5-78b1-4bc6-9821-09a1faa36136",
                "image_display_url": None,
                "image_url": None,
                "name": "ckan_admin",
                "number_created_packages": 1,
                "state": "active",
                "sysadmin": True,
            }
        ],
        "tags": [],
        "groups": [],
    }
    created = create_organization_raw(ckan_instance, org, datamanager="ckan_admin")
    assert created["name"] == "new_test_organization"
    ckan_instance.delete_organization(created["id"])
    ckan_instance.purge_organization(created["id"])


@pytest.mark.impure
def test_create_missing_project(ckan_instance, ckan_envvars, ckan_setup_data):
    new_proj_name = "new_test_group"
    proj = {
        "approval_status": "approved",
        "created": "2024-02-19T14:31:11.072921",
        "description": "This is a test project",
        "display_name": "test_project",
        "id": "d031afd5-8de5-4b23-aa98-cef196f38614",
        "image_display_url": "https://images.squarespace-cdn.com/content/v1/5cf6c4ed5171fc0001b43190/1611069934488-IVPUR8YDTK9G6R7O3F16/paul.png",
        "image_url": "https://images.squarespace-cdn.com/content/v1/5cf6c4ed5171fc0001b43190/1611069934488-IVPUR8YDTK9G6R7O3F16/paul.png",
        "is_organization": False,
        "name": new_proj_name,
        "num_followers": 0,
        "package_count": 0,
        "state": "active",
        "title": "test_project",
        "type": "group",
        "users": [
            {
                "about": None,
                "activity_streams_email_notifications": False,
                "capacity": "admin",
                "created": "2024-01-25T09:33:37.949157",
                "display_name": "ckan_admin",
                "email_hash": "7c512b13badc48258d94ef72d1c8889a",
                "fullname": None,
                "id": "a60585d5-78b1-4bc6-9821-09a1faa36136",
                "image_display_url": None,
                "image_url": None,
                "name": "ckan_admin",
                "number_created_packages": 1,
                "state": "active",
                "sysadmin": True,
            }
        ],
        "extras": [],
        "packages": [],
        "tags": [],
        "groups": [],
    }
    proj_id = create_project_raw(ckan_instance, proj)["id"]
    assert ckan_instance.get_project(proj_id)["name"] == "new_test_group"
    ckan_instance.delete_project(proj_id)
    ckan_instance.purge_project(proj_id)


@pytest.mark.impure
def test_create_missing_organization_projects_variables(
    ckan_instance, ckan_envvars, ckan_setup_data
):
    for to_create in collect_missing_entity(
        ckan_instance,
        {
            "missing": {
                "projects": [ckan_envvars["test_project"]],
                "organization": [ckan_envvars["test_organization"]],
            }
        },
    ):
        if to_create["entity"].startswith("project"):
            ckan_instance.delete_project(to_create["data"]["id"])
            ckan_instance.purge_project(to_create["data"]["id"])
        elif to_create["entity"].startswith("organization"):
            package_id = ckan_instance.get_package(
                ckan_envvars["test_package"], filter_fields="id"
            )["id"]
            ckan_instance.delete_package(package_id)
            ckan_instance.delete_organization(to_create["data"]["id"])
            ckan_instance.purge_organization(to_create["data"]["id"])

        create_missing_organization_projects_variables(
            ckan_instance, **to_create, org_data_manager="ckan_admin"
        )


@pytest.mark.impure
def test_create_missing_package(ckan_instance, ckan_envvars, ckan_setup_data):
    author = "this is free text"
    maintainer = "ckan_admin"  # this is not free text, user must be registered in ckan
    usage_contact = (
        "ckan_admin"  # this is not free text, user must be registered in ckan
    )

    pkg = {
        "author": [author],
        "author_email": "example@localhost.ch",
        "creator_user_id": "a60585d5-78b1-4bc6-9821-09a1faa36136",
        "id": "bfc07875-6cad-4af7-a5e9-e7318955c0fc",
        "isopen": False,
        "license_id": None,
        "license_title": None,
        "maintainer": "ckan_admin",
        "maintainer_email": None,
        "metadata_created": "2024-02-19T14:33:50.683412",
        "metadata_modified": "2024-02-19T14:33:50.974051",
        "name": "new_test_package",
        "notes": "some_note",
        "num_resources": 1,
        "num_tags": 1,
        "organization": {
            "id": "1924b2db-e39f-42c2-a640-7709b98bf95f",
            "name": "test_organization",
            "title": "Test_Organization",
            "type": "organization",
            "description": "This is my organization.",
            "image_url": "https://www.techrepublic.com/wp-content/uploads/2017/03/meme05.jpg",
            "created": "2024-02-19T14:33:50.593412",
            "is_organization": True,
            "approval_status": "approved",
            "state": "active",
        },
        "owner_org": "THIS NEEDS TO BE GOTTEN",
        "private": False,
        "publicationlink": "",
        "review_level": "none",
        "reviewed_by": "",
        "spatial": '{"type": "Point", "coordinates": [8.609776496939471, 47.40384502816517]}',
        "state": "active",
        "status": "incomplete",
        "tags_string": "some_tag",
        "timerange": ["*"],
        "title": "Test_Package",
        "type": "dataset",
        "url": None,
        "usage_contact": "ckan_admin",
        "variables": ["none"],
        "version": None,
        "resources": [
            {
                "cache_last_updated": None,
                "cache_url": None,
                "created": "2024-02-19T14:33:50.982168",
                "datastore_active": False,
                "description": None,
                "format": "JPEG",
                "hash": "",
                "id": "a63195ed-7802-4193-b24e-fec209d55ecc",
                "last_modified": None,
                "metadata_modified": "2024-02-19T14:33:50.978578",
                "mimetype": "image/jpeg",
                "mimetype_inner": None,
                "name": "test_resource_link",
                "package_id": "bfc07875-6cad-4af7-a5e9-e7318955c0fc",
                "position": 0,
                "resource_type": "Dataset",
                "restricted_level": "public",
                "size": None,
                "state": "active",
                "url": "https://static.demilked.com/wp-content/uploads/2021/07/60ed37b256b80-it-rage-comics-memes-reddit-60e6fee1e7dca__700.jpg",
                "url_type": None,
            }
        ],
        "tags": [
            {
                "display_name": "some_tag",
                "id": "817c946f-d18e-493a-a2b7-ff22d1e4d650",
                "name": "some_tag",
                "state": "active",
                "vocabulary_id": None,
            }
        ],
        "groups": [
            {
                "description": "This is a test project",
                "display_name": "test_project",
                "id": "THIS NEEDS TO BE GOTTEN",
                "image_display_url": "https://images.squarespace-cdn.com/content/v1/5cf6c4ed5171fc0001b43190/1611069934488-IVPUR8YDTK9G6R7O3F16/paul.png",
                "name": "test_group",
                "title": "test_project",
            }
        ],
        "relationships_as_subject": [],
        "relationships_as_object": [],
    }

    res = create_package_raw(
        ckan_instance, pkg, new_maintainer=maintainer, new_usage_contact=usage_contact
    )
    assert res["name"] == "new_test_package"


@pytest.mark.impure
def test_create_resource_raw(tmp_path, ckan_instance, ckan_envvars, ckan_setup_data):
    (file_path := tmp_path / "file_0").write_text("fdsffsd")

    data = [
        {
            "cache_last_updated": None,
            "cache_url": None,
            "created": "2024-02-19T17:40:03.085674",
            "datastore_active": False,
            "description": None,
            "format": "JPEG",
            "hash": "",
            "id": "0b481119-6275-44fc-a8cd-b8ede5878786",
            "last_modified": None,
            "metadata_modified": "2024-02-19T17:40:03.489792",
            "mimetype": "image/jpeg",
            "mimetype_inner": None,
            "name": "test_resource_link",
            "package_id": "5e0c1fb8-3d03-4d9e-8b59-3626ddc84493",
            "position": 0,
            "resource_type": "Dataset",
            "restricted_level": "public",
            "size": None,
            "state": "active",
            "url": "https://static.demilked.com/wp-content/uploads/2021/07/60ed37b256b80-it-rage-comics-memes-reddit-60e6fee1e7dca__700.jpg",
            "url_type": None,
        },
        {
            "cache_last_updated": None,
            "cache_url": None,
            "citation": "",
            "created": "2024-02-19T17:40:03.495710",
            "datastore_active": False,
            "description": "",
            "format": "",
            "hash": "absd",
            "hashtype": "sha256",
            "id": "f44fbccb-28fc-448c-9210-883f5ead53e9",
            "last_modified": "2024-02-19T17:40:03.472381",
            "metadata_modified": "2024-02-19T17:40:03.827869",
            "mimetype": None,
            "mimetype_inner": None,
            "name": "file_0",
            "package_id": "5e0c1fb8-3d03-4d9e-8b59-3626ddc84493",
            "position": 1,
            "resource_type": "Dataset",
            "restricted_level": "public",
            "size": 1048576,
            "state": "active",
            "url": "https://localhost:8443/dataset/5e0c1fb8-3d03-4d9e-8b59-3626ddc84493/resource/f44fbccb-28fc-448c-9210-883f5ead53e9/download/file_0",
            "url_type": "upload",
        },
    ]

    _ = ckan_instance.create_package(
        **{
            "title": "New Test_Package",
            "name": "new-test-package",
            "private": False,
            "description": "This is my package.",
            "author": "ckan_admin",
            "author_email": "example@localhost.ch",
            "state": "active",
            "type": "dataset",
            "owner_org": "test_organization",
            "reviewed_by": "",
            "maintainer": "ckan_admin",
            "usage_contact": "ckan_admin",
            "notes": "some_note",
            "review_level": "none",
            "spatial": '{"type": "Point", "coordinates": [8.609776496939471, 47.40384502816517]}',
            "status": "incomplete",
            "tags_string": "some_tag",
            "timerange": "*",
            "variables": "none",
        }
    )

    for resource in data:
        if resource_is_link(resource):
            upload_func = upload_resource_link_via_api
        else:
            upload_func = upload_resource_file_via_api

        create_resource_raw(
            ckan_api_input={
                "token": ckan_instance.token,
                "server": ckan_instance.server,
                "verify_certificate": ckan_instance.verify,
            },
            secure_interface_input={"key": "this does not matter for this test"},
            ckan_storage_path="this does not matter for this test",
            package_name="new-test-package",
            metadata=resource,
            file_path=file_path.as_posix(),
            upload_func=upload_func,
            progressbar=False,
            is_link=resource_is_link(resource),
        )

    assert len(ckan_instance.get_package("new-test-package")["resources"]) == 2
