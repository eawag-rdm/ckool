import pytest

from ckool import HASH_TYPE, UPLOAD_IN_PROGRESS_STRING
from ckool.other.caching import read_cache
from ckool.other.hashing import get_hash_func
from ckool.templates import (
    get_upload_func,
    handle_file,
    handle_upload,
    hash_remote,
    resource_integrity_between_ckan_instances_intact,
    resource_integrity_remote_intact,
    upload_resource_file_via_api,
    upload_resource_file_via_scp,
)

hasher = get_hash_func(HASH_TYPE)


@pytest.mark.impure
def test_resource_integrity_between_ckan_instances_intact(
    tmp_path, ckan_instance, ckan_envvars, ckan_setup_data
):
    (f := tmp_path / "file_abc.txt").write_text("test")
    kwargs = {
        "file": f,
        "package_id": ckan_envvars["test_package"],
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
        ckan_envvars["test_package"],
        f.name,
    )


@pytest.mark.impure
def test_upload_resource_file_via_api(
    tmp_path, ckan_instance, ckan_envvars, ckan_setup_data
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
            "token": ckan_instance.token,
            "server": ckan_instance.server,
            "verify_certificate": ckan_instance.verify,
        },
        filepath=f,
        package_name=ckan_envvars["test_package"],
        metadata=meta,
        progressbar=False,
    )

    resources = ckan_instance.get_package(
        package_name=ckan_envvars["test_package"], filter_fields=["resources"]
    )["resources"]

    assert any([r["name"] == f.name for r in resources])


@pytest.mark.impure
def test_upload_resource_file_via_scp(
    tmp_path, ckan_instance, secure_interface_input_args, ckan_envvars, ckan_setup_data
):
    (f := tmp_path / "file.txt").write_text("test")
    meta = {
        "size": f.stat().st_size,
        "hash": hasher(f),
        "format": f.suffix[1:],
        "hashtype": HASH_TYPE,
    }

    ckan_input_args = {
        "token": ckan_instance.token,
        "server": ckan_instance.server,
        "verify_certificate": ckan_instance.verify,
    }
    abc = upload_resource_file_via_scp(
        ckan_api_input=ckan_input_args,
        secure_interface_input=secure_interface_input_args,
        ckan_storage_path=ckan_envvars["storage_path"],
        package_name=ckan_envvars["test_package"],
        filepath=f,
        metadata=meta,
    )

    resources = ckan_instance.get_package(
        package_name=ckan_envvars["test_package"], filter_fields=["resources"]
    )["resources"]

    assert any([r["name"] == f.name for r in resources])
    assert resource_integrity_remote_intact(
        ckan_api_input=ckan_input_args,
        secure_interface_input=secure_interface_input_args,
        ckan_storage_path=ckan_envvars["storage_path"],
        package_name=ckan_envvars["test_package"],
        resource_id_or_name=f.name,
    )


@pytest.mark.impure
def test_upload_func_chosen_api_file(
    tmp_path, ckan_instance, secure_interface_input_args, ckan_envvars, ckan_setup_data
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
    )
    ckan_input_args = {
        "token": ckan_instance.token,
        "server": ckan_instance.server,
        "verify_certificate": ckan_instance.verify,
    }
    upload(
        ckan_api_input=ckan_input_args,
        secure_interface_input=secure_interface_input_args,
        ckan_storage_path=ckan_envvars["storage_path"],
        package_name=ckan_envvars["test_package"],
        filepath=f,
        metadata=meta,
        progressbar=True,
    )

    resources = ckan_instance.get_package(
        package_name=ckan_envvars["test_package"], filter_fields=["resources"]
    )["resources"]

    assert any([r["name"] == f.name for r in resources])
    assert resource_integrity_remote_intact(
        ckan_api_input=ckan_input_args,
        secure_interface_input=secure_interface_input_args,
        ckan_storage_path=ckan_envvars["storage_path"],
        package_name=ckan_envvars["test_package"],
        resource_id_or_name=f.name,
    )


@pytest.mark.impure
def test_upload_func_chosen_scp(
    tmp_path, ckan_instance, secure_interface_input_args, ckan_envvars, ckan_setup_data
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
    )
    ckan_input_args = {
        "token": ckan_instance.token,
        "server": ckan_instance.server,
        "verify_certificate": ckan_instance.verify,
    }
    upload(
        ckan_api_input=ckan_input_args,
        secure_interface_input=secure_interface_input_args,
        ckan_storage_path=ckan_envvars["storage_path"],
        package_name=ckan_envvars["test_package"],
        filepath=f,
        metadata=meta,
        progressbar=True,
    )

    resources = ckan_instance.get_package(
        package_name=ckan_envvars["test_package"], filter_fields=["resources"]
    )["resources"]

    assert any([r["name"] == f.name for r in resources])
    assert resource_integrity_remote_intact(
        ckan_api_input=ckan_input_args,
        secure_interface_input=secure_interface_input_args,
        ckan_storage_path=ckan_envvars["storage_path"],
        package_name=ckan_envvars["test_package"],
        resource_id_or_name=f.name,
    )


@pytest.mark.impure
def test_hash_remote(
    tmp_path, ckan_instance, secure_interface_input_args, ckan_envvars, ckan_setup_data
):
    (f := tmp_path / "file.txt").write_text("test")
    meta = {
        "file": f,
        "package_id": ckan_envvars["test_package"],
        "size": f.stat().st_size,
        "hash": hasher(f),
        "format": f.suffix[1:],
        "hashtype": HASH_TYPE,
    }
    _ = ckan_instance.create_resource_of_type_file(**meta)
    ckan_input_args = {
        "token": ckan_instance.token,
        "server": ckan_instance.server,
        "verify_certificate": ckan_instance.verify,
    }
    hashed_remotely = hash_remote(
        ckan_api_input=ckan_input_args,
        secure_interface_input=secure_interface_input_args,
        ckan_storage_path=ckan_envvars["storage_path"],
        package_name=ckan_envvars["test_package"],
        resource_id_or_name=f.name,
    )
    hashed_locally = ckan_instance.get_resource_meta(
        package_name=ckan_envvars["test_package"],
        resource_id_or_name=f.name,
    )["hash"]

    assert hashed_locally == hashed_remotely


# @pytest.mark.impure
def test_handle_upload(
    tmp_path,
    ckan_instance,
    secure_interface_input_args,
    ckan_envvars,
    ckan_setup_data,
    config_section_instance,
):
    hash_func = get_hash_func(HASH_TYPE)

    (tmp_path / ckan_envvars["test_package"]).mkdir()
    (file_1 := (tmp_path / ckan_envvars["test_package"] / "file_1.txt")).write_text(
        "abc"
    )
    (file_2 := (tmp_path / ckan_envvars["test_package"] / "file_2.txt")).write_text(
        "def"
    )
    (file_3 := (tmp_path / ckan_envvars["test_package"] / "file_3.txt")).write_text(
        "ghi"
    )
    cache_1 = handle_file(file=file_1, hash_func=hash_func)
    cache_2 = handle_file(file=file_2, hash_func=hash_func)
    cache_3 = handle_file(file=file_3, hash_func=hash_func)

    ckan_instance.create_resource_of_type_file(
        package_id=ckan_envvars["test_package"], **read_cache(cache_1)
    )
    meta_2 = read_cache(cache_2)
    meta_2["hash"] = UPLOAD_IN_PROGRESS_STRING
    ckan_instance.create_resource_of_type_file(
        package_id=ckan_envvars["test_package"], **meta_2
    )

    uploaded = handle_upload(
        package_name=ckan_envvars["test_package"],
        package_folder=tmp_path / ckan_envvars["test_package"],
        verify=False,
        parallel=False,
        progressbar=True,
        **config_section_instance,
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

    for resource in ckan_instance.get_package(ckan_envvars["test_package"])["resources"]:
        assert resource["hash"] != UPLOAD_IN_PROGRESS_STRING
