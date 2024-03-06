import json
from copy import deepcopy

import pytest

from ckool import (
    LOCAL_DOI_STORE_AFFILIATION_FILE_NAME,
    LOCAL_DOI_STORE_DOI_FILE_NAME,
    LOCAL_DOI_STORE_ORCIDS_FILE_NAME,
    LOCAL_DOI_STORE_RELATED_PUBLICATIONS_FILE_NAME,
)
from ckool.ckan.publishing import (
    any_missing_organization_projects_variables,
    collect_missing_entity,
    create_missing_organization_projects_variables,
    create_organization_raw,
    create_package_raw,
    create_project_raw,
    create_resource_raw,
    enrich_and_store_metadata,
    patch_resource_metadata_raw,
    pre_publication_checks,
    update_datacite_doi,
)
from ckool.datacite.doi_store import LocalDoiStore
from ckool.other.utilities import resource_is_link
from ckool.templates import upload_resource_file_via_api, upload_resource_link_via_api
from tests.ckool.data.inputs.ckan_entity_data import (
    full_organization_data,
    full_package_data,
    full_project_data,
    package_data,
    test_resources,
)


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
        ckan_instance_destination=ckan_instance,
        package_metadata=package_metadata,
        projects_to_publish=[ckan_envvars["test_project"]],
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
        projects_to_publish=["non-existent-group"],
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

    result = pre_publication_checks(
        ckan_instance_destination=ckan_instance,
        package_metadata=package_metadata,
        projects_to_publish=[],
    )
    assert result == {
        "missing": {
            "package": ["non-existent-package"],
            "organization": ["non-existent-organization"],
            "resources": ["non-existent-resource"],
            "projects": [],
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

    result = pre_publication_checks(
        ckan_instance_destination=ckan_instance, package_metadata=package_metadata
    )
    assert result == {
        "missing": {
            "package": ["non-existent-package"],
            "organization": ["non-existent-organization"],
            "resources": ["non-existent-resource"],
            "projects": [],
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
    full_organization_data["name"] = new_org_name

    created = create_organization_raw(
        ckan_instance, full_organization_data, datamanager="ckan_admin"
    )
    assert created["name"] == "new_test_organization"
    ckan_instance.delete_organization(created["id"])
    ckan_instance.purge_organization(created["id"])


@pytest.mark.impure
def test_create_missing_project(ckan_instance, ckan_envvars, ckan_setup_data):
    new_proj_name = "new_test_group"
    full_project_data["name"] = new_proj_name

    proj_id = create_project_raw(
        ckan_instance, full_project_data, prepare_for_publication=False
    )["id"]
    assert ckan_instance.get_project(proj_id)["name"] == "new_test_group"
    ckan_instance.delete_project(proj_id)
    ckan_instance.purge_project(proj_id)

    proj_id = create_project_raw(
        ckan_instance, full_project_data, prepare_for_publication=True
    )["id"]
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
            ckan_instance,
            **to_create,
            org_data_manager="ckan_admin",
            prepare_for_publication=False,
        )


@pytest.mark.impure
def test_create_missing_package(ckan_instance, ckan_envvars, ckan_setup_data):
    pkg = deepcopy(full_package_data)
    res = create_package_raw(
        ckan_instance, ckan_instance, pkg, prepare_for_publication=False
    )

    assert res["name"] == "new_test_package"
    ckan_instance.delete_package(res["id"])

    with pytest.raises(ValueError):
        create_package_raw(
            ckan_instance_source=ckan_instance,
            ckan_instance_target=ckan_instance,
            data=pkg,
            prepare_for_publication=True,
        )


@pytest.mark.impure
def test_create_resource_raw(tmp_path, ckan_instance, ckan_envvars, ckan_setup_data):
    (file_path := tmp_path / "file_0").write_text("fdsffsd")
    pkg_data = deepcopy(package_data)
    pkg_data["name"] = "new-test-package"
    _ = ckan_instance.create_package(**pkg_data)

    for resource in test_resources:
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
            prepare_for_publication=False,
        )

    assert len(ckan_instance.get_package("new-test-package")["resources"]) == 2


@pytest.mark.impure
def test_patch_resource_raw(tmp_path, ckan_instance, ckan_envvars, ckan_setup_data):
    (file_path := tmp_path / "file_0").write_text("fdsffsd")
    pkg_data = deepcopy(package_data)
    pkg_data["name"] = "new-test-package"
    _ = ckan_instance.create_package(**pkg_data)
    ckan_input_args = {
        "token": ckan_instance.token,
        "server": ckan_instance.server,
        "verify_certificate": ckan_instance.verify,
    }
    for resource in test_resources:
        if resource_is_link(resource):
            upload_func = upload_resource_link_via_api
        else:
            upload_func = upload_resource_file_via_api

        create_resource_raw(
            ckan_api_input=ckan_input_args,
            secure_interface_input={"key": "this does not matter for this test"},
            ckan_storage_path="this does not matter for this test",
            package_name="new-test-package",
            metadata=resource,
            file_path=file_path.as_posix(),
            upload_func=upload_func,
            progressbar=False,
            is_link=resource_is_link(resource),
            prepare_for_publication=False,
        )

        resource["hashtype"] = "md5"
        patch_resource_metadata_raw(
            ckan_input_args,
            "new-test-package",
            resource["name"],
            resource,
            is_link=resource_is_link(resource),
            prepare_for_publication=False,
        )

    assert len(ckan_instance.get_package("new-test-package")["resources"]) == 2


@pytest.mark.impure
def test_enrich_and_store_metadata_1(
    tmp_path,
    ckan_instance,
    secure_interface_input_args,
    ckan_envvars,
    json_test_data,
    ckan_setup_data,
):
    (tmp_path / "person-2323" / ckan_envvars["test_package"]).mkdir(parents=True)

    lds = LocalDoiStore(tmp_path)
    lds.write(
        name="person-2323",
        package=ckan_envvars["test_package"],
        filename_content_map={
            "doi.txt": "10.45934/25AZ53",
        },
    )

    # The metadata seems to be the one from eric open.
    metadata = ckan_instance.get_package(ckan_envvars["test_package"])
    metadata["author"] = ["Foerster, Christian <christian.foerster@eawag.ch>"]
    metadata["geographic_name"] = []

    files = enrich_and_store_metadata(
        metadata=metadata,
        local_doi_store_instance=lds,
        package_name=ckan_envvars["test_package"],
        ask_orcids=False,
        ask_affiliations=False,
        ask_related_identifiers=False,
    )
    assert files["json"].exists()
    assert files["xml"].exists()


@pytest.mark.impure
def test_enrich_and_store_metadata_2(
    tmp_path,
    ckan_instance,
    ckan_envvars,
    json_test_data,
    ckan_setup_data,
):
    (tmp_path / "person-2323" / ckan_envvars["test_package"]).mkdir(parents=True)

    lds = LocalDoiStore(tmp_path)
    lds.write(
        name="person-2323",
        package=ckan_envvars["test_package"],
        filename_content_map={
            "doi.txt": "10.45934/25AZ53",
        },
    )

    # The metadata seems to be the one from eric open.
    metadata = ckan_instance.get_package(ckan_envvars["test_package"])
    metadata["author"] = ["Foerster, Christian <christian.foerster@eawag.ch>"]
    metadata["geographic_name"] = []

    files = enrich_and_store_metadata(
        metadata=metadata,
        local_doi_store_instance=lds,
        package_name=ckan_envvars["test_package"],
        ask_orcids=False,
        ask_affiliations=False,
        ask_related_identifiers=False,
    )
    assert files["json"].exists()
    assert files["xml"].exists()


@pytest.mark.impure
def test_update_datacite_doi(
    tmp_path,
    datacite_instance,
    ckan_instance,
    ckan_envvars,
    json_test_data,
    ckan_setup_data,
):
    (_dir := tmp_path / "person-2323" / ckan_envvars["test_package"]).mkdir(
        parents=True
    )
    _doi = f"{datacite_instance.prefix}/25AZ53"
    lds = LocalDoiStore(tmp_path)
    lds.write(
        name="person-2323",
        package=ckan_envvars["test_package"],
        filename_content_map={
            _dir / LOCAL_DOI_STORE_DOI_FILE_NAME: _doi,
            _dir
            / LOCAL_DOI_STORE_ORCIDS_FILE_NAME: json.dumps(
                json_test_data["orcids"], indent=2
            ),
            _dir
            / LOCAL_DOI_STORE_AFFILIATION_FILE_NAME: json.dumps(
                json_test_data["affiliations"], indent=2
            ),
            _dir
            / LOCAL_DOI_STORE_RELATED_PUBLICATIONS_FILE_NAME: json.dumps(
                json_test_data["related_publications"], indent=2
            ),
        },
    )

    # The metadata seems to be the one from eric open.
    metadata = ckan_instance.get_package(ckan_envvars["test_package"])
    metadata["author"] = ["Foerster, Christian <christian.foerster@eawag.ch>"]
    metadata["geographic_name"] = []

    _ = enrich_and_store_metadata(
        metadata=metadata,
        local_doi_store_instance=lds,
        package_name=ckan_envvars["test_package"],
        ask_orcids=False,
        ask_affiliations=False,
        ask_related_identifiers=False,
    )

    datacite_instance.doi_reserve(_doi)
    success = update_datacite_doi(
        datacite_api_instance=datacite_instance,
        local_doi_store_instance=lds,
        package_name=ckan_envvars["test_package"],
    )
    assert success
    assert datacite_instance.doi_retrieve(_doi)

    datacite_instance.doi_delete(_doi)
