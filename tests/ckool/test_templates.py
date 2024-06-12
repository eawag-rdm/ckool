import pathlib
from copy import deepcopy

import ckanapi
import pytest
from conftest import ckan_instance_names_of_fixtures

from ckool import HASH_TYPE, UPLOAD_IN_PROGRESS_STRING
from ckool.other.caching import read_cache
from ckool.other.hashing import get_hash_func
from ckool.templates import (
    get_upload_func,
    handle_file,
    handle_upload_all,
    hash_remote,
    resource_integrity_between_ckan_instances_intact,
    resource_integrity_remote_intact,
    upload_resource_file_via_api,
    upload_resource_file_via_scp,
    wrapped_upload,
)
from tests.ckool.data.inputs.ckan_entity_data import package_data

hasher = get_hash_func(HASH_TYPE)


@pytest.mark.impure
def test_resource_integrity_between_ckan_instances_intact(
    tmp_path, ckan_instance, ckan_entities, ckan_setup_data
):
    (f := tmp_path / "file_abc.txt").write_text("test")
    kwargs = {
        "file": f,
        "package_id": ckan_entities["test_package"],
        "size": f.stat().st_size,
        "hash": hasher(f),
        "format": f.suffix[1:],
        "hashtype": HASH_TYPE,
    }
    ckan_instance.create_resource_of_type_file(**kwargs)

    assert resource_integrity_between_ckan_instances_intact(
        {
            "token": ckan_instance.token,
            "server": ckan_instance.server,
            "verify_certificate": ckan_instance.verify,
        },
        {
            "token": ckan_instance.token,
            "server": ckan_instance.server,
            "verify_certificate": ckan_instance.verify,
        },
        ckan_entities["test_package"],
        f.name,
    )


@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.impure
def test_upload_resource_file_via_api(
    cki, tmp_path, dynamic_ckan_instance, ckan_entities, dynamic_ckan_setup_data
):
    (f := tmp_path / "file.txt").write_text("test")
    meta = {
        "size": f.stat().st_size,
        "hash": hasher(f),
        "format": f.suffix[1:],
        "hashtype": HASH_TYPE,
    }

    upload_resource_file_via_api(
        ckan_api_input={
            "token": dynamic_ckan_instance.token,
            "server": dynamic_ckan_instance.server,
            "verify_certificate": dynamic_ckan_instance.verify,
        },
        filepath=f,
        package_name=ckan_entities["test_package"],
        metadata=meta,
        progressbar=False,
    )

    resources = dynamic_ckan_instance.get_package(
        package_name=ckan_entities["test_package"], filter_fields=["resources"]
    )["resources"]

    assert any([r["name"] == f.name for r in resources])


@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.impure
def test_upload_resource_file_via_scp(
    cki,
    tmp_path,
    dynamic_ckan_instance,
    dynamic_config,
    ckan_entities,
    dynamic_ckan_setup_data,
):
    (f := tmp_path / "file.txt").write_text("test")
    meta = {
        "size": f.stat().st_size,
        "hash": hasher(f),
        "format": f.suffix[1:],
        "hashtype": HASH_TYPE,
    }

    ckan_input_args = {
        "token": dynamic_ckan_instance.token,
        "server": dynamic_ckan_instance.server,
        "verify_certificate": dynamic_ckan_instance.verify,
    }
    _ = upload_resource_file_via_scp(
        ckan_api_input=ckan_input_args,
        secure_interface_input=dynamic_config["ckan_server"][0],
        ckan_storage_path=dynamic_config["other"][0]["ckan_storage_path"],
        package_name=ckan_entities["test_package"],
        filepath=f,
        metadata=meta,
    )

    resources = dynamic_ckan_instance.get_package(
        package_name=ckan_entities["test_package"], filter_fields=["resources"]
    )["resources"]

    assert any([r["name"] == f.name for r in resources])
    assert resource_integrity_remote_intact(
        ckan_api_input=ckan_input_args,
        secure_interface_input=dynamic_config["ckan_server"][0],
        ckan_storage_path=dynamic_config["other"][0]["ckan_storage_path"],
        package_name=ckan_entities["test_package"],
        resource_id_or_name=f.name,
    )


@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.impure
def test_upload_func_chosen_api_file(
    cki,
    tmp_path,
    dynamic_ckan_instance,
    dynamic_config,
    ckan_entities,
    dynamic_ckan_setup_data,
):
    (f := tmp_path / "file.txt").write_text("test")
    meta = {
        "size": f.stat().st_size,
        "hash": hasher(f),
        "format": f.suffix[1:],
        "hashtype": HASH_TYPE,
    }

    upload = get_upload_func(
        file_sizes=[50 * 1024**2, 100 * 1024**2],
        space_available_on_server_root_disk=1000 * 1024**2,
        parallel_upload=False,
        factor=2,
        is_link=False,
        force_scp=False,
    )
    ckan_input_args = {
        "token": dynamic_ckan_instance.token,
        "server": dynamic_ckan_instance.server,
        "verify_certificate": dynamic_ckan_instance.verify,
    }
    upload(
        ckan_api_input=ckan_input_args,
        secure_interface_input=dynamic_config["ckan_server"][0],
        ckan_storage_path=dynamic_config["other"][0]["ckan_storage_path"],
        package_name=ckan_entities["test_package"],
        filepath=f,
        metadata=meta,
        progressbar=True,
    )

    resources = dynamic_ckan_instance.get_package(
        package_name=ckan_entities["test_package"], filter_fields=["resources"]
    )["resources"]

    assert any([r["name"] == f.name for r in resources])
    assert resource_integrity_remote_intact(
        ckan_api_input=ckan_input_args,
        secure_interface_input=dynamic_config["ckan_server"][0],
        ckan_storage_path=dynamic_config["other"][0]["ckan_storage_path"],
        package_name=ckan_entities["test_package"],
        resource_id_or_name=f.name,
    )


@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.impure
def test_upload_func_chosen_scp(
    cki,
    tmp_path,
    dynamic_ckan_instance,
    dynamic_config,
    ckan_entities,
    dynamic_ckan_setup_data,
):
    (f := tmp_path / "file.txt").write_text("test")
    meta = {
        "size": f.stat().st_size,
        "hash": hasher(f),
        "format": f.suffix[1:],
        "hashtype": HASH_TYPE,
    }

    upload = get_upload_func(
        file_sizes=[50 * 1024**2, 100 * 1024**2],
        space_available_on_server_root_disk=100 * 1024**2,
        parallel_upload=False,
        factor=2,
        is_link=False,
        force_scp=False,
    )
    ckan_input_args = {
        "token": dynamic_ckan_instance.token,
        "server": dynamic_ckan_instance.server,
        "verify_certificate": dynamic_ckan_instance.verify,
    }
    upload(
        ckan_api_input=ckan_input_args,
        secure_interface_input=dynamic_config["ckan_server"][0],
        ckan_storage_path=dynamic_config["other"][0]["ckan_storage_path"],
        package_name=ckan_entities["test_package"],
        filepath=f,
        metadata=meta,
        progressbar=True,
    )

    resources = dynamic_ckan_instance.get_package(
        package_name=ckan_entities["test_package"], filter_fields=["resources"]
    )["resources"]

    assert any([r["name"] == f.name for r in resources])
    assert resource_integrity_remote_intact(
        ckan_api_input=ckan_input_args,
        secure_interface_input=dynamic_config["ckan_server"][0],
        ckan_storage_path=dynamic_config["other"][0]["ckan_storage_path"],
        package_name=ckan_entities["test_package"],
        resource_id_or_name=f.name,
    )


@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.impure
def test_hash_remote(
    cki,
    tmp_path,
    dynamic_ckan_instance,
    dynamic_config,
    ckan_entities,
    dynamic_ckan_setup_data,
):
    (f := tmp_path / "file.txt").write_text("test")
    meta = {
        "file": f,
        "package_id": ckan_entities["test_package"],
        "size": f.stat().st_size,
        "hash": hasher(f),
        "format": f.suffix[1:],
        "hashtype": HASH_TYPE,
    }
    _ = dynamic_ckan_instance.create_resource_of_type_file(**meta)
    ckan_input_args = {
        "token": dynamic_ckan_instance.token,
        "server": dynamic_ckan_instance.server,
        "verify_certificate": dynamic_ckan_instance.verify,
    }
    hashed_remotely = hash_remote(
        ckan_api_input=ckan_input_args,
        secure_interface_input=dynamic_config["ckan_server"][0],
        ckan_storage_path=dynamic_config["other"][0]["ckan_storage_path"],
        package_name=ckan_entities["test_package"],
        resource_id_or_name=f.name,
    )
    hashed_locally = dynamic_ckan_instance.get_resource_meta(
        package_name=ckan_entities["test_package"],
        resource_id_or_name=f.name,
    )["hash"]

    assert hashed_locally == hashed_remotely


@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.impure
def test_handle_upload(
    cki,
    tmp_path,
    dynamic_ckan_instance,
    ckan_entities,
    dynamic_ckan_setup_data,
    dynamic_config_section_instance,
):
    hash_func = get_hash_func(HASH_TYPE)

    (tmp_path / ckan_entities["test_package"]).mkdir()
    (file_1 := (tmp_path / ckan_entities["test_package"] / "file_1.txt")).write_text(
        "abc"
    )
    (file_2 := (tmp_path / ckan_entities["test_package"] / "file_2.txt")).write_text(
        "def"
    )
    (file_3 := (tmp_path / ckan_entities["test_package"] / "file_3.txt")).write_text(
        "ghi"
    )
    cache_1 = handle_file(file=file_1, hash_func=hash_func)
    cache_2 = handle_file(file=file_2, hash_func=hash_func)
    _ = handle_file(file=file_3, hash_func=hash_func)

    dynamic_ckan_instance.create_resource_of_type_file(
        package_id=ckan_entities["test_package"], **read_cache(cache_1)
    )
    meta_2 = read_cache(cache_2)
    meta_2["hash"] = UPLOAD_IN_PROGRESS_STRING
    dynamic_ckan_instance.create_resource_of_type_file(
        package_id=ckan_entities["test_package"], **meta_2
    )

    uploaded = handle_upload_all(
        package_name=ckan_entities["test_package"],
        package_folder=tmp_path / ckan_entities["test_package"],
        verify=False,
        parallel=False,
        progressbar=True,
        force_scp=False,
        **dynamic_config_section_instance,
    )

    for info in uploaded:
        del info["id"]
        assert info in [
            {
                "name": "file_3.txt",
                "status": "normal",
            },
            {
                "name": "file_2.txt",
                "status": "replaced",
            },
            {
                "name": "file_1.txt",
                "status": "skipped",
            },
        ]

    for resource in dynamic_ckan_instance.get_package(ckan_entities["test_package"])[
        "resources"
    ]:
        assert resource["hash"] != UPLOAD_IN_PROGRESS_STRING


@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.impure
def test_handle_upload_all_too_many_cache_files(
    cki,
    tmp_path,
    ckan_entities,
    dynamic_ckan_setup_data,
    dynamic_config_section_instance,
):
    hash_func = get_hash_func(HASH_TYPE)

    (tmp_path / ckan_entities["test_package"]).mkdir()
    (file_1 := (tmp_path / ckan_entities["test_package"] / "file_1.txt")).write_text(
        "abc"
    )
    (file_2 := (tmp_path / ckan_entities["test_package"] / "file_2.txt")).write_text(
        "def"
    )
    (file_3 := (tmp_path / ckan_entities["test_package"] / "file_3.txt")).write_text(
        "ghi"
    )
    _ = handle_file(file=file_1, hash_func=hash_func)
    _ = handle_file(file=file_2, hash_func=hash_func)
    _ = handle_file(file=file_3, hash_func=hash_func)

    file_3.unlink()

    uploaded = handle_upload_all(
        package_name=ckan_entities["test_package"],
        package_folder=tmp_path / ckan_entities["test_package"],
        verify=False,
        parallel=False,
        progressbar=True,
        force_scp=False,
        **dynamic_config_section_instance,
    )
    assert len(uploaded) == 2


@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.impure
@pytest.mark.parametrize(
    "upload_func", [upload_resource_file_via_api, upload_resource_file_via_scp]
)
def test_wrapped_upload_for_both_upload_types(
    cki,
    tmp_path,
    dynamic_ckan_instance,
    dynamic_config,
    ckan_entities,
    dynamic_ckan_setup_data,
    upload_func,
):
    (f := tmp_path / "file.txt").write_text("test")
    meta = {
        "file": f,
        "size": f.stat().st_size,
        "hash": hasher(f),
        "format": f.suffix[1:],
        "hashtype": HASH_TYPE.value,
    }
    wrapped_upload(
        meta=meta,
        package_name=ckan_entities["test_package"],
        ckan_instance=dynamic_ckan_instance,
        cfg_other={
            "ckan_storage_path": dynamic_config["other"][0]["ckan_storage_path"]
        },
        cfg_ckan_api={
            "token": dynamic_ckan_instance.token,
            "server": dynamic_ckan_instance.server,
            "verify_certificate": dynamic_ckan_instance.verify,
        },
        cfg_secure_interface=dynamic_config["ckan_server"][0],
        upload_func=upload_func,
        progressbar=True,
    )

    resources = dynamic_ckan_instance.get_package(
        package_name=ckan_entities["test_package"]
    )["resources"]

    resource = None
    for resource in resources:
        if resource["url_type"] == "upload":
            break
    assert resource["name"] == f.name
    assert pathlib.Path(resource["url"]).name == f.name
    assert resource["hash"] == meta["hash"]
    assert resource["hashtype"] == meta["hashtype"]


@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.impure
@pytest.mark.parametrize(
    "spatial",
    (
        "",
        '{"type": "Point", "coordinates": [8.609776496939471, 47.40384502816517]}',
        '{"type": "MultiPoint", "coordinates": [[100.0, 0.0],[101.0, 1.0]]}',
        '{"type": "LineString", "coordinates": [[100.0, 0.0], [101.0, 1.0]]}',
        '{"type": "MultiLineString", "coordinates": [[[100.0, 0.0], [101.0, 1.0]], [[102.0, 2.0], [103.0, 3.0]]]}',
        '{"type": "Polygon", "coordinates": [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]]}',
        '{"type": "MultiPolygon", "coordinates": [[[[102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0]]], [[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]], [[100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2]]]]}',
    ),
)
def test_create_package_different_spatial_formats_valid(
    cki,
    tmp_path,
    dynamic_ckan_instance,
    dynamic_config,
    ckan_entities,
    dynamic_ckan_setup_data,
    spatial,
):
    pkg = deepcopy(package_data)
    pkg["name"] = "new_package"
    pkg["owner_org"] = ckan_entities["test_organization"]
    pkg["spatial"] = spatial
    dynamic_ckan_instance.create_package(**pkg)


@pytest.mark.parametrize("cki", ckan_instance_names_of_fixtures)
@pytest.mark.impure
@pytest.mark.parametrize("spatial", ("{}",))
def test_create_package_different_spatial_formats_invalid(
    cki,
    tmp_path,
    dynamic_ckan_instance,
    dynamic_config,
    ckan_entities,
    dynamic_ckan_setup_data,
    spatial,
):
    """This test is required for the old ckan"""
    pkg = deepcopy(package_data)
    pkg["name"] = "new_package"
    pkg["owner_org"] = ckan_entities["test_organization"]
    pkg["spatial"] = spatial
    with pytest.raises(ckanapi.errors.ValidationError):
        dynamic_ckan_instance.create_package(**pkg)
