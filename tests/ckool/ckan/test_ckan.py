import json
import time
from copy import deepcopy

import ckanapi
import pytest

from ckool import HASH_TYPE
from ckool.ckan.ckan import filter_resources, CKAN
from ckool.other.hashing import get_hash_func
from conftest import ckan_instances
from tests.ckool.data.inputs.ckan_entity_data import package_data

hasher = get_hash_func(HASH_TYPE)


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

    # required for eric open
    if data.get("tags"):
        for i in range(len(data["tags"])):
            del data["tags"][i]["id"]
    if data.get("creator_user_id"):
        del data["creator_user_id"]
    if data.get("usage_contact"):
        del data["usage_contact"]

    return data


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_get_all_packages(cki, request):
    ckan_instance: CKAN = request.getfixturevalue(cki)
    assert ckan_instance.get_all_packages()["count"] == 0


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_get_all_packages2(cki, dynamic_ckan_instance, dynamic_ckan_setup_data):
    assert dynamic_ckan_instance.get_all_packages()["count"] == 1


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_get_all_packages_with_data(cki, dynamic_ckan_instance, dynamic_ckan_setup_data):
    assert dynamic_ckan_instance.get_all_packages()["count"] == 1


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_get_package(cki, dynamic_ckan_instance, dynamic_ckan_setup_data, ckan_entities, valid_outputs):
    data = dynamic_ckan_instance.get_package(ckan_entities["test_package"])
    with (valid_outputs / "package_data_from_ckan.json").open() as f:
        right_data = json.load(f)
    assert _delete_resource_ids_and_times(data) == _delete_resource_ids_and_times(
        right_data
    )


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_get_package_metadata_package_does_not_exist(cki, dynamic_ckan_instance, dynamic_ckan_setup_data):
    with pytest.raises(ckanapi.errors.NotFound):
        dynamic_ckan_instance.get_package("this-package-name-does-not-exist")


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_get_package_metadata_filtered_1(cki, dynamic_ckan_instance, dynamic_ckan_setup_data, ckan_entities):
    data = dynamic_ckan_instance.get_package(
        ckan_entities["test_package"], filter_fields=["maintainer", "author"]
    )
    assert len(data) == 2


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_reorder_package_resources(
    tmp_path, ckan_entities, cki, dynamic_ckan_instance, dynamic_ckan_setup_data
):
    files = [
        tmp_path / "z.ending",
        tmp_path / "az.txt",
        tmp_path / "fsq.abc",
        tmp_path / "ba.as.as.ds",
    ]
    for idx, f in enumerate(files):
        f.write_text(f"file {idx}")
        meta = {
            "file": f,
            "package_id": ckan_entities["test_package"],
            "size": f.stat().st_size,
            "hash": hasher(f),
            "format": f.suffix[1:],
            "hashtype": HASH_TYPE,
        }
        dynamic_ckan_instance.create_resource_of_type_file(**meta)
    resource_names_initial = [
        r["name"]
        for r in dynamic_ckan_instance.get_package(
            ckan_entities["test_package"], ["resources"]
        )["resources"]
    ]
    dynamic_ckan_instance.reorder_package_resources(ckan_entities["test_package"])
    resource_names_ordered = [
        r["name"]
        for r in dynamic_ckan_instance.get_package(
            ckan_entities["test_package"], ["resources"]
        )["resources"]
    ]
    assert sorted(resource_names_initial) == resource_names_ordered


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_reorder_package_resources_with_readme(
    tmp_path, ckan_entities, cki, dynamic_ckan_instance, dynamic_ckan_setup_data
):
    files = [
        tmp_path / "z.ending",
        tmp_path / "az.txt",
        tmp_path / "fsq.abc",
        tmp_path / "ba.as.as.ds",
        tmp_path / "readme.md",
    ]
    for idx, f in enumerate(files):
        f.write_text(f"file {idx}")
        meta = {
            "file": f,
            "package_id": ckan_entities["test_package"],
            "size": f.stat().st_size,
            "hash": hasher(f),
            "format": f.suffix[1:],
            "hashtype": HASH_TYPE,
        }
        dynamic_ckan_instance.create_resource_of_type_file(**meta)

    dynamic_ckan_instance.reorder_package_resources(ckan_entities["test_package"])
    resource_names_ordered = [
        r["name"]
        for r in dynamic_ckan_instance.get_package(
            ckan_entities["test_package"], ["resources"]
        )["resources"]
    ]

    assert [
        "readme.md",
        "az.txt",
        "ba.as.as.ds",
        "fsq.abc",
        "test_resource_link",
        "z.ending",
    ] == resource_names_ordered


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_reorder_package_resources_with_readme_raises(
    tmp_path, dynamic_ckan_instance, ckan_entities, cki, dynamic_ckan_setup_data
):
    files = [
        tmp_path / "z.ending",
        tmp_path / "az.txt",
        tmp_path / "fsq.abc",
        tmp_path / "ba.as.as.ds",
        tmp_path / "readme.md",
        tmp_path / "readme.txt",
    ]
    for idx, f in enumerate(files):
        f.write_text(f"file {idx}")
        meta = {
            "file": f,
            "package_id": ckan_entities["test_package"],
            "size": f.stat().st_size,
            "hash": hasher(f),
            "format": f.suffix[1:],
            "hashtype": HASH_TYPE,
        }
        dynamic_ckan_instance.create_resource_of_type_file(**meta)
    with pytest.raises(ValueError):
        dynamic_ckan_instance.reorder_package_resources(ckan_entities["test_package"])


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_get_package_metadata_filtered_2(
    tmp_path, dynamic_ckan_instance, ckan_entities, cki, dynamic_ckan_setup_data
):
    sha256 = get_hash_func(HASH_TYPE)
    test_file = tmp_path / "test_file.ending"
    test_file.write_text("some text")
    dynamic_ckan_instance.create_resource_of_type_file(
        file=test_file,
        package_id=ckan_entities["test_package"],
        hash=sha256(test_file),
        size=test_file.stat().st_size,
        progressbar=False,
    )
    data = dynamic_ckan_instance.get_resource_meta(
        ckan_entities["test_package"], resource_id_or_name=test_file.name
    )
    _ = dynamic_ckan_instance.get_local_resource_path(
        ckan_entities["test_package"],
        resource_id_or_name=test_file.name,
        ckan_storage_path="/var/lib/ckan",
    )
    # remote_hash = os.popen(f"docker exec ckan sha256sum {path}").read().split(" ")[0]
    assert (
        data["hash"]
        == "b94f6f125c79e3a5ffaa826f584c10d52ada669e6762051b826b55776d05aed2"
    )


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_get_local_resource_path(
    tmp_path, dynamic_ckan_instance, ckan_entities, cki, dynamic_ckan_setup_data
):
    test_file = tmp_path / "test_file.ending"
    test_file.touch()
    dynamic_ckan_instance.create_resource_of_type_file(
        file=test_file,
        package_id=ckan_entities["test_package"],
        hash="abc",
        size=test_file.stat().st_size,
        progressbar=False,
    )
    relative_resource_path = dynamic_ckan_instance.get_local_resource_path(
        package_name=ckan_entities["test_package"],
        resource_id_or_name=test_file.name,
    )

    resource_path = dynamic_ckan_instance.get_local_resource_path(
        package_name=ckan_entities["test_package"],
        resource_id_or_name=test_file.name,
        ckan_storage_path="/var/lib/ckan/resources/",
    )
    assert resource_path == "/var/lib/ckan/resources/" + relative_resource_path

    resource_path = dynamic_ckan_instance.get_local_resource_path(
        package_name=ckan_entities["test_package"],
        resource_id_or_name=test_file.name,
        ckan_storage_path="/var/lib/ckan/resources",
    )
    assert resource_path == "/var/lib/ckan/resources/" + relative_resource_path

    resource_path = dynamic_ckan_instance.get_local_resource_path(
        package_name=ckan_entities["test_package"],
        resource_id_or_name=test_file.name,
        ckan_storage_path="/var/lib/ckan/",
    )
    assert resource_path == "/var/lib/ckan/resources/" + relative_resource_path

    resource_path = dynamic_ckan_instance.get_local_resource_path(
        package_name=ckan_entities["test_package"],
        resource_id_or_name=test_file.name,
        ckan_storage_path="/var/lib/ckan",
    )
    assert resource_path == "/var/lib/ckan/resources/" + relative_resource_path


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_resource_patch(dynamic_ckan_instance, ckan_entities, cki, dynamic_ckan_setup_data):
    new_name = "new_name"
    resource_id = dynamic_ckan_instance.get_package(ckan_entities["test_package"])["resources"][
        0
    ]["id"]
    dynamic_ckan_instance.patch_resource_metadata(
        resource_id=resource_id, resource_data_to_update={"name": new_name}
    )
    name_in_ckan = dynamic_ckan_instance.get_package(ckan_entities["test_package"])[
        "resources"
    ][0]["name"]
    assert new_name == name_in_ckan


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_update_package_metadata(dynamic_ckan_instance, ckan_entities, cki, dynamic_ckan_setup_data):
    data = dynamic_ckan_instance.get_package(ckan_entities["test_package"])

    new_message = "this field was changed"
    original_message = data["notes"]

    data["notes"] = new_message
    dynamic_ckan_instance.update_package_metadata(data)

    changed_data = dynamic_ckan_instance.get_package(ckan_entities["test_package"])
    assert changed_data["notes"] == new_message

    changed_data["notes"] = original_message
    dynamic_ckan_instance.update_package_metadata(changed_data)

    data = dynamic_ckan_instance.get_package(ckan_entities["test_package"])
    assert data["notes"] == original_message


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_patch_package_metadata(dynamic_ckan_instance, ckan_entities, cki, dynamic_ckan_setup_data):
    data = dynamic_ckan_instance.get_package(ckan_entities["test_package"])
    original_message = data["notes"]
    new_message = "this field was changed"

    data = dynamic_ckan_instance.patch_package_metadata(
        ckan_entities["test_package"], {"notes": new_message}
    )
    assert data["notes"] == new_message

    data = dynamic_ckan_instance.patch_package_metadata(
        ckan_entities["test_package"], {"notes": original_message}
    )
    assert data["notes"] == original_message


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_resource_create_link(dynamic_ckan_instance, ckan_entities, cki, dynamic_ckan_setup_data):
    dynamic_ckan_instance.create_resource_of_type_link(
        **{
            "package_id": ckan_entities["test_package"],
            "name": "test_resource_new",
            "resource_type": "Dataset",
            "restricted_level": "public",
            "url": "https://static.demilked.com/wp-content/uploads/2021/07/60ed37b256b80-it-rage-comics-memes-reddit-60e6fee1e7dca__700.jpg",
        }
    )


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_resource_create_file_minimal(
    dynamic_ckan_instance, ckan_entities, cki, dynamic_ckan_setup_data, small_file
):
    dynamic_ckan_instance.create_resource_of_type_file(
        **{
            "file": small_file,
            "package_id": ckan_entities["test_package"],
            "size": small_file.stat().st_size,
            "hash": hasher(small_file),
        },
    )


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_resource_create_file_maximal(
    dynamic_ckan_instance, ckan_entities, cki, dynamic_ckan_setup_data, small_file
):
    dynamic_ckan_instance.create_resource_of_type_file(
        **{
            "file": small_file,
            "package_id": ckan_entities["test_package"],
            "size": small_file.stat().st_size,
            "hash": hasher(small_file),
            "citation": "Some text here",
            "description": "A very long description",
            "format": small_file.suffix.strip("."),
            "hashtype": "sha256",
            "resource_type": "Dataset",
            "restricted_level": "public",
            "state": "active",
        },
    )


@pytest.mark.slow
@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_download_package_with_resources_sequential(
    tmp_path, dynamic_ckan_instance, ckan_entities, cki, dynamic_ckan_setup_data, add_file_resources
):
    add_file_resources(
        dynamic_ckan_instance,
        [
            100 * 1024**2,
            100 * 1024**2,
            100 * 1024**2,
            10 * 1024**2,
        ]
    )

    data = dynamic_ckan_instance.get_package(ckan_entities["test_package"])
    assert len(data["resources"]) == 5

    st = time.time()
    downloaded_files_1 = dynamic_ckan_instance.download_package_with_resources(
        ckan_entities["test_package"], destination=tmp_path
    )
    en = time.time()
    print(f"Sequential download took {en-st}s.")

    st = time.time()
    downloaded_files_2 = dynamic_ckan_instance.download_package_with_resources(
        ckan_entities["test_package"], destination=tmp_path, parallel=True
    )
    en = time.time()
    print(f"Parallel download took {en-st}s.")
    assert downloaded_files_1 == downloaded_files_2
    assert len(list(tmp_path.iterdir())) == 5


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_filter_resource(tmp_path, dynamic_ckan_instance, ckan_entities, cki, dynamic_ckan_setup_data):
    for i in range(3):
        (file := tmp_path / f"file_{i}.txt").write_text(f"{i}")
        dynamic_ckan_instance.create_resource_of_type_file(
            **{
                "file": file,
                "package_id": ckan_entities["test_package"],
                "size": file.stat().st_size,
                "hash": hasher(file),
            },
        )

    data = dynamic_ckan_instance.get_package(ckan_entities["test_package"])
    resource_ids = [i["id"] for i in data["resources"]]
    resource_names = [i["name"] for i in data["resources"]]

    assert filter_resources(data, resources_to_exclude=resource_ids)["resources"] == []
    assert (
        filter_resources(data, resources_to_exclude=resource_names)["resources"] == []
    )
    assert filter_resources(data, resources_to_exclude=resource_ids[1:])[
        "resources"
    ] == [data["resources"][0]]

    with pytest.raises(ValueError):
        filter_resources(data, resources_to_exclude=["undefined_name"])
        filter_resources(
            data, resources_to_exclude=[resource_ids[0], resource_names[0]]
        )


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_filter_resource_requires_resource_ids(
    tmp_path, dynamic_ckan_instance, ckan_entities, cki, dynamic_ckan_setup_data
):
    for i in range(2):
        (file := tmp_path / "file.txt").write_text(f"{i}")
        dynamic_ckan_instance.create_resource_of_type_file(
            **{
                "file": file,
                "package_id": ckan_entities["test_package"],
                "size": file.stat().st_size,
                "hash": hasher(file),
            },
        )
        (file := tmp_path / "file_1.txt").write_text(f"1_{i}")
        dynamic_ckan_instance.create_resource_of_type_file(
            **{
                "file": file,
                "package_id": ckan_entities["test_package"],
                "size": file.stat().st_size,
                "hash": hasher(file),
            },
        )

    data = dynamic_ckan_instance.get_package(ckan_entities["test_package"])
    resource_ids = [i["id"] for i in data["resources"]]
    resource_names = [i["name"] for i in data["resources"]]

    assert filter_resources(data, resources_to_exclude=resource_ids)["resources"] == []

    with pytest.raises(ValueError):
        filter_resources(data, resources_to_exclude=resource_names)


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_organization_existence(dynamic_ckan_instance, ckan_entities, cki, dynamic_ckan_setup_data):
    dynamic_ckan_instance.get_organization(ckan_entities["test_organization"])
    with pytest.raises(ckanapi.errors.NotFound):
        dynamic_ckan_instance.get_organization("does-not-exist")


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_get_group(dynamic_ckan_instance, ckan_entities, cki, dynamic_ckan_setup_data):
    assert (
        len(dynamic_ckan_instance.get_project(ckan_entities["test_project"])["packages"]) == 0
    )
    with pytest.raises(ckanapi.errors.NotFound):
        dynamic_ckan_instance.get_project("absas")


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_add_package_to_project(dynamic_ckan_instance, ckan_entities, cki, dynamic_ckan_setup_data):
    dynamic_ckan_instance.add_package_to_project(
        ckan_entities["test_package"], ckan_entities["test_project"]
    )
    assert (
        len(dynamic_ckan_instance.get_project(ckan_entities["test_project"])["packages"]) == 1
    )
    assert len(dynamic_ckan_instance.get_package(ckan_entities["test_package"])["groups"]) == 1


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_get_user(dynamic_ckan_instance, cki, dynamic_ckan_setup_data):
    assert dynamic_ckan_instance.get_user("ckan_admin")


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_create_package_with_additional_field(dynamic_ckan_instance, cki, dynamic_ckan_setup_data):
    pkg_name = "another_new_package"
    pkg = deepcopy(package_data)

    pkg.update({"geographic_name": ["Switzerland"]})
    pkg["name"] = pkg_name

    dynamic_ckan_instance.create_package(**pkg)

    assert "geographic_name" in dynamic_ckan_instance.get_package(pkg_name).keys()


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_package_delete_purge(dynamic_ckan_instance, ckan_entities, cki, dynamic_ckan_setup_data):
    pkg = deepcopy(package_data)

    dynamic_ckan_instance.delete_package(ckan_entities["test_package"])
    assert (
        dynamic_ckan_instance.get_package(ckan_entities["test_package"])["state"] == "deleted"
    )

    dynamic_ckan_instance.create_package(**pkg)
    dynamic_ckan_instance.delete_package(ckan_entities["test_package"])
    dynamic_ckan_instance.purge_package(ckan_entities["test_package"])
    with pytest.raises(ckanapi.errors.NotFound):
        dynamic_ckan_instance.get_package(ckan_entities["test_package"])


@pytest.mark.parametrize('cki', ckan_instances)
@pytest.mark.impure
def test_package_delete_delete_purge(dynamic_ckan_instance, ckan_entities, cki, dynamic_ckan_setup_data):
    dynamic_ckan_instance.delete_package(ckan_entities["test_package"])
    dynamic_ckan_instance.delete_package(ckan_entities["test_package"])
    assert (
        dynamic_ckan_instance.get_package(ckan_entities["test_package"])["state"] == "deleted"
    )
    dynamic_ckan_instance.purge_package(ckan_entities["test_package"])
    with pytest.raises(ckanapi.errors.NotFound):
        dynamic_ckan_instance.get_package(ckan_entities["test_package"])
