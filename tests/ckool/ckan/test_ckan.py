import json
import time

import ckanapi
import pytest

from ckool import HASH_TYPE
from ckool.ckan.ckan import filter_resources
from ckool.other.hashing import get_hash_func

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
    return data


@pytest.mark.impure
def test_get_all_packages(ckan_instance, ckan_envvars):
    assert ckan_instance.get_all_packages()["count"] == 1


@pytest.mark.impure
def test_get_package(ckan_instance, ckan_envvars, ckan_setup_data, valid_outputs):
    data = ckan_instance.get_package(ckan_envvars["test_package"])
    with (valid_outputs / "package_data_from_ckan.json").open() as f:
        right_data = json.load(f)
    assert _delete_resource_ids_and_times(data) == _delete_resource_ids_and_times(
        right_data
    )


@pytest.mark.impure
def test_get_package_metadata_package_does_not_exist(ckan_instance, ckan_setup_data):
    with pytest.raises(ckanapi.errors.NotFound):
        ckan_instance.get_package("this-package-name-does-not-exist")


@pytest.mark.impure
def test_get_package_metadata_filtered(ckan_instance, ckan_envvars, ckan_setup_data):
    data = ckan_instance.get_package(
        ckan_envvars["test_package"], filter_fields=["maintainer", "author"]
    )
    assert len(data) == 2


@pytest.mark.impure
def test_reorder_package_resources(
    tmp_path, ckan_instance, ckan_envvars, ckan_setup_data
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
            "package_id": ckan_envvars["test_package"],
            "file_size": f.stat().st_size,
            "file_hash": hasher(f),
            "file_format": f.suffix[1:],
            "hash_type": HASH_TYPE,
        }
        ckan_instance.create_resource_of_type_file(**meta)
    resource_names_initial = [
        r["name"]
        for r in ckan_instance.get_package(ckan_envvars["test_package"], ["resources"])[
            "resources"
        ]
    ]
    ckan_instance.reorder_package_resources(ckan_envvars["test_package"])
    resource_names_ordered = [
        r["name"]
        for r in ckan_instance.get_package(ckan_envvars["test_package"], ["resources"])[
            "resources"
        ]
    ]
    assert sorted(resource_names_initial) == resource_names_ordered


@pytest.mark.impure
def test_get_package_metadata_filtered(
    tmp_path, ckan_instance, ckan_envvars, ckan_setup_data
):
    sha256 = get_hash_func(HASH_TYPE)
    test_file = tmp_path / "test_file.ending"
    test_file.write_text("some text")
    ckan_instance.create_resource_of_type_file(
        file=test_file,
        package_id=ckan_envvars["test_package"],
        file_hash=sha256(test_file),
        file_size=test_file.stat().st_size,
        progressbar=False,
    )
    data = ckan_instance.get_resource_meta(
        ckan_envvars["test_package"], resource_name=test_file.name
    )
    path = ckan_instance.get_local_resource_path(
        ckan_envvars["test_package"],
        resource_name=test_file.name,
        ckan_storage_path="/var/lib/ckan",
    )
    # remote_hash = os.popen(f"docker exec ckan sha256sum {path}").read().split(" ")[0]
    assert (
        data["hash"]
        == "b94f6f125c79e3a5ffaa826f584c10d52ada669e6762051b826b55776d05aed2"
    )


@pytest.mark.impure
def test_get_local_resource_path(
    tmp_path, ckan_instance, ckan_envvars, ckan_setup_data
):
    test_file = tmp_path / "test_file.ending"
    test_file.touch()
    ckan_instance.create_resource_of_type_file(
        file=test_file,
        package_id=ckan_envvars["test_package"],
        file_hash="abc",
        file_size=test_file.stat().st_size,
        progressbar=False,
    )
    relative_resource_path = ckan_instance.get_local_resource_path(
        package_name=ckan_envvars["test_package"],
        resource_name=test_file.name,
    )

    resource_path = ckan_instance.get_local_resource_path(
        package_name=ckan_envvars["test_package"],
        resource_name=test_file.name,
        ckan_storage_path="/var/lib/ckan/resources/",
    )
    assert resource_path == "/var/lib/ckan/resources/" + relative_resource_path

    resource_path = ckan_instance.get_local_resource_path(
        package_name=ckan_envvars["test_package"],
        resource_name=test_file.name,
        ckan_storage_path="/var/lib/ckan/resources",
    )
    assert resource_path == "/var/lib/ckan/resources/" + relative_resource_path

    resource_path = ckan_instance.get_local_resource_path(
        package_name=ckan_envvars["test_package"],
        resource_name=test_file.name,
        ckan_storage_path="/var/lib/ckan/",
    )
    assert resource_path == "/var/lib/ckan/resources/" + relative_resource_path

    resource_path = ckan_instance.get_local_resource_path(
        package_name=ckan_envvars["test_package"],
        resource_name=test_file.name,
        ckan_storage_path="/var/lib/ckan",
    )
    assert resource_path == "/var/lib/ckan/resources/" + relative_resource_path


@pytest.mark.impure
def test_resource_patch(ckan_instance, ckan_envvars, ckan_setup_data):
    new_name = "new_name"
    resource_id = ckan_instance.get_package(ckan_envvars["test_package"])["resources"][
        0
    ]["id"]
    ckan_instance.patch_resource_metadata(
        resource_id=resource_id, resource_data_to_update={"name": new_name}
    )
    name_in_ckan = ckan_instance.get_package(ckan_envvars["test_package"])["resources"][
        0
    ]["name"]
    assert new_name == name_in_ckan


@pytest.mark.impure
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


@pytest.mark.impure
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


@pytest.mark.impure
def test_resource_create_link(ckan_instance, ckan_envvars, ckan_setup_data):
    ckan_instance.create_resource_of_type_link(
        **{
            "package_id": ckan_envvars["test_package"],
            "name": "test_resource_new",
            "resource_type": "Dataset",
            "restricted_level": "public",
            "url": "https://static.demilked.com/wp-content/uploads/2021/07/60ed37b256b80-it-rage-comics-memes-reddit-60e6fee1e7dca__700.jpg",
        }
    )


@pytest.mark.impure
def test_resource_create_file_minimal(
    ckan_instance, ckan_envvars, ckan_setup_data, small_file
):
    ckan_instance.create_resource_of_type_file(
        **{
            "file": small_file,
            "package_id": ckan_envvars["test_package"],
            "file_size": small_file.stat().st_size,
            "file_hash": hasher(small_file),
        },
    )


@pytest.mark.impure
def test_resource_create_file_maximal(
    ckan_instance, ckan_envvars, ckan_setup_data, small_file
):
    ckan_instance.create_resource_of_type_file(
        **{
            "file": small_file,
            "package_id": ckan_envvars["test_package"],
            "file_size": small_file.stat().st_size,
            "file_hash": hasher(small_file),
            "citation": "Some text here",
            "description": "A very long description",
            "file_format": small_file.suffix.strip("."),
            "hash_type": "sha256",
            "resource_type": "Dataset",
            "restricted_level": "public",
            "state": "active",
        },
    )


@pytest.mark.slow
@pytest.mark.impure
def test_download_package_with_resources_sequential(
    tmp_path, ckan_instance, ckan_envvars, ckan_setup_data, add_file_resources
):
    add_file_resources(
        [
            100 * 1024**2,
            100 * 1024**2,
            100 * 1024**2,
            140 * 1024**2,
            123 * 1024**2,
            120 * 1024**2,
            100 * 1024**2,
            10 * 1024**2,
        ]
    )

    data = ckan_instance.get_package(ckan_envvars["test_package"])
    assert len(data["resources"]) == 9

    st = time.time()
    downloaded_files_1 = ckan_instance.download_package_with_resources(
        ckan_envvars["test_package"], destination=tmp_path
    )
    en = time.time()
    print(f"Sequential download took {en-st}s.")

    st = time.time()
    downloaded_files_2 = ckan_instance.download_package_with_resources(
        ckan_envvars["test_package"], destination=tmp_path, parallel=True
    )
    en = time.time()
    print(f"Parallel download took {en-st}s.")
    assert downloaded_files_1 == downloaded_files_2
    assert len(list(tmp_path.iterdir())) == 9


@pytest.mark.impure
def test_filter_resource(tmp_path, ckan_instance, ckan_envvars, ckan_setup_data):
    for i in range(3):
        (file := tmp_path / f"file_{i}.txt").write_text(f"{i}")
        ckan_instance.create_resource_of_type_file(
            **{
                "file": file,
                "package_id": ckan_envvars["test_package"],
                "file_size": file.stat().st_size,
                "file_hash": hasher(file),
            },
        )

    data = ckan_instance.get_package(ckan_envvars["test_package"])
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


@pytest.mark.impure
def test_filter_resource_requires_resource_ids(
    tmp_path, ckan_instance, ckan_envvars, ckan_setup_data
):
    for i in range(2):
        (file := tmp_path / "file.txt").write_text(f"{i}")
        ckan_instance.create_resource_of_type_file(
            **{
                "file": file,
                "package_id": ckan_envvars["test_package"],
                "file_size": file.stat().st_size,
                "file_hash": hasher(file),
            },
        )
        (file := tmp_path / "file_1.txt").write_text(f"1_{i}")
        ckan_instance.create_resource_of_type_file(
            **{
                "file": file,
                "package_id": ckan_envvars["test_package"],
                "file_size": file.stat().st_size,
                "file_hash": hasher(file),
            },
        )

    data = ckan_instance.get_package(ckan_envvars["test_package"])
    resource_ids = [i["id"] for i in data["resources"]]
    resource_names = [i["name"] for i in data["resources"]]

    assert filter_resources(data, resources_to_exclude=resource_ids)["resources"] == []

    with pytest.raises(ValueError):
        filter_resources(data, resources_to_exclude=resource_names)
